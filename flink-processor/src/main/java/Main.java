/*
===============================================================
    Main.java
===============================================================

File ini merupakan "otak" dari pipeline Apache Flink.

Pipeline yang dibuat:
    Kafka
        │
        ▼
    Weather Object
        │
        ▼
    Group By City
        │
        ▼
    Window 10 Detik
        │
        ▼
    Hitung Average Temperature
        │
        ▼
    PostgreSQL
*/

// Import Library 
import org.apache.flink.api.common.eventtime.WatermarkStrategy; // Digunakan untuk menentukan strategi watermark. Pada project ini kita tidak menggunakan Event Time, sehingga memakai WatermarkStrategy.noWatermarks().
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;   // Digunakan untuk konfigurasi koneksi JDBC.
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;    // Digunakan untuk konfigurasi proses insert JDBC.
import org.apache.flink.connector.jdbc.JdbcSink;                // Sink JDBC. Berfungsi mengirim hasil Flink ke PostgreSQL.
import org.apache.flink.connector.kafka.source.KafkaSource;     // Source Kafka. Digunakan Flink untuk membaca data dari Kafka.
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;   // Menentukan offset awal Kafka.
import org.apache.flink.streaming.api.datastream.DataStreamSource;                          // Stream hasil pembacaan dari Source.
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;               // Environment utama Apache Flink. Semua job akan dijalankan di dalam Environment ini.
import org.apache.kafka.common.TopicPartition;                                              // Import ini sebenarnya tidak digunakan. Bisa dihapus tanpa mempengaruhi program.

import org.apache.flink.api.common.functions.AggregateFunction;                             // AggregateFunction digunakan untuk menghitung rata-rata.
import org.apache.flink.api.java.tuple.Tuple2;                                              // Tuple2 digunakan untuk menyimpan dua buah nilai.
import org.apache.flink.streaming.api.datastream.DataStream;                                // Representasi stream data di Flink.
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;    // Window berdasarkan Processing Time.
import org.apache.flink.streaming.api.windowing.time.Time;                                  // Representasi waktu.
import org.apache.flink.api.common.functions.MapFunction;                                   // Digunakan untuk transformasi map().

import java.util.Arrays;                                        // Import ini juga tidak digunakan.
import java.util.HashSet;                                       // Import ini juga tidak digunakan.

// Class
public class Main {
    static final String BROKERS = "kafka:9092";     // Alamat Kafka Broker. Karena seluruh service berada di Docker Network, hostname yang digunakan adalah nama servicenya.
    static final String TOPIC = "weather";          // Nama topic Kafka.

    // Main Method
    public static void main(String[] args) throws Exception {

        // Membuat StreamExecutionEnvironment. 
        // Environment ini merupakan "tempat kerja" Flink. Semua Source, semua Transformation, semua Sink akan didaftarkan ke Environment ini.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("Environment created");


        // Membuat Kafka Source
        // KafkaSource merupakan komponen yang bertugas membaca data dari Kafka.
        // Generic <Weather> -> setiap message Kafka nantinya akan diubah menjadi object Weather.
        KafkaSource<Weather> source = KafkaSource.<Weather>builder()
                .setBootstrapServers(BROKERS)                               // Menentukan alamat Kafka Broker.
                .setProperty("partition.discovery.interval.ms", "1000")     // Mengecek partition baru setiap 1000 ms. Jika suatu saat topic memiliki partition baru, Flink dapat mendeteksinya.
                .setTopics(TOPIC)                                           // Set topic yang akan dibaca.
                .setGroupId("weather-consumer")                             // Consumer Group Kafka. Kafka akan menyimpan offset berdasarkan nama group ini.
                .setStartingOffsets(OffsetsInitializer.earliest())          // Membaca data mulai dari offset paling awal. Jika topic sudah memiliki data sebelumnya,maka semuanya akan dibaca.
                .setValueOnlyDeserializer(new WeatherDeserializationSchema())   // Mengubah JSON Kafka menjadi Object Weather.
                .build();       // Builder selesai.

                
                
        // Membuat DataStream dari Kafka Source
        /*
            env.fromSource() digunakan untuk menghubungkan KafkaSource yang telah dibuat sebelumnya dengan Apache Flink.
            Setelah baris ini dieksekusi, Flink memiliki sebuah DataStream yang berisi object Weather.

            Secara konsep:
            Kafka
               │
               ▼
            Weather Object
               │
               ▼
            DataStream<Weather>
        */
        DataStreamSource<Weather> kafka = env.fromSource(
            source,                                 // Source Kafka yang telah dikonfigurasi
            WatermarkStrategy.noWatermarks(),       // Tidak menggunakan Watermark. Project ini menggunakan Processing Time, sehingga Event Time tidak diperlukan.
            "kafka"     // Nama operator. Nama ini biasanya muncul pada Flink UI sehingga memudahkan debugging
        );
        System.out.println("Kafka source created");


        // Menampilkan seluruh data yang diterima dari Kafka.
        /*
            Contoh output:
            Weather{
                city='Jakarta',
                temperature=31.5
            }
        */
        kafka.print();      // Fungsi print() biasanya hanya digunakan saat development atau debugging.


        // Menghitung rata-rata temperature
        /*
            Pipeline berikut akan melakukan:
            
            Data dari Kafka
                    │
                    ▼
            Group berdasarkan city
                    │
                    ▼
            Window 10 detik
                    │
                    ▼ 
            Hitung Average
        */
        DataStream<Tuple2<MyAverage, Double>> averageTemperatureStream = kafka
                .keyBy(myEvent -> myEvent.city)                             // keyBy() -> Mengelompokkan data berdasarkan city. Semua proses aggregate dilakukan PER GROUP
                .window(                                                    // window() -> Menentukan kapan proses aggregate dilakukan. 
                    TumblingProcessingTimeWindows.of(Time.seconds(10))      // Pada project ini digunakan Tumbling Processing Time Window sepanjang 10 detik. Artinya: 00-10 detik dihitung average. 10-20 detik dihitung average lagi, dan seterusnya
                )     
                .aggregate(new AverageAggregator());                        // aggregate() -> Menjalankan AverageAggregator. Class ini akan menghitung jumlah data total temperature average


        // Mengubah bentuk output aggregate
        /*
            Output dari AverageAggregator adalah: Tuple2<MyAverage, Double>
            dimana:
            - MyAverage
                berisi city, sum, count
            - Double
                berisi average
        
            Namun untuk disimpan ke PostgreSQL kita hanya membutuhkan:
            - city
            - average
        
            Maka dilakukan transformasi menggunakan map().
        */
        DataStream<Tuple2<String, Double>>cityAndValueStream = averageTemperatureStream     // Output dari AverageAggregator adalah: Tuple2<MyAverage, Double>
            .map(new MapFunction<Tuple2<MyAverage, Double>, Tuple2<String, Double>>() {     // map() -> Dipanggil setiap kali hasil aggregate selesai.
                @Override
                public Tuple2<String, Double> map(
                        Tuple2<MyAverage, Double> input     
                ) throws Exception {
                    return new Tuple2<>(        // Return: ("Jakarta",31.7)
                            input.f0.city,      // input.f0 -> object MyAverage
                            input.f1            // input.f1 -> average
                    );
                }
            });
        System.out.println("Aggregation created");

        // Menampilkan hasil aggregate.
        /*
            Contoh:
                MyAverage{
                    city='Jakarta',
                    count=10,
                    sum=310
                }
                Average: 31
        */
        averageTemperatureStream.print();


        // Menampilkan hasil akhir setelah map().
        /*
            Contoh:
                Jakarta
                31
        */
        cityAndValueStream.print();


        // Menampilkan hasil akhir setelah proses map()
        /*
            Contoh output:
                (Jakarta,31.4)
                (Bandung,27.8)
                (Surabaya,33.2)
        */
        cityAndValueStream.print(); // print() digunakan untuk debugging. Data yang tampil pada console merupakan data yang nantinya juga akan dikirim ke PostgreSQL.


        // JDBC Sink
        // Pada project ini destination-nya adalah PostgreSQL. Setiap hasil average temperature akan di-INSERT ke dalam database.
        cityAndValueStream.addSink(     // Sink merupakan tujuan akhir (destination) dari DataStream.
            JdbcSink.sink(
                // Query SQL yang akan dijalankan.
                "INSERT INTO weather (city, average_temperature) VALUES (?, ?)",
                
                // Lambda Function. Bertugas mengisi parameter SQL.
                (statement, event) -> {     
                    statement.setString(1, event.f0);      // event.f0 -> city
                    statement.setDouble(2, event.f1);      // event.f1 -> average temperature
                },

                // Konfigurasi JDBC Execution
                JdbcExecutionOptions.builder()
                    .withBatchSize(1000)        // Batch Size. Maksimal 1000 record sebelum dikirim ke database. Tujuannya mengurangi jumlah query.
                    .withBatchIntervalMs(200)   // Batch Interval. Jika belum mencapai 1000 record, maka setelah 200 ms data tetap dikirim ke database.
                    .withMaxRetries(5)          // Retry maksimal sebanyak 5 kali.
                    .build(),                   // Build object konfigurasi.

                // Konfigurasi Koneksi PostgreSQL
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    // .withUrl("jdbc:postgresql://docker.for.windows.host.internal:5438/postgres")     // URL lama. Digunakan ketika PostgreSQL berjalan langsung di Windows.
                    .withUrl("jdbc:postgresql://postgres:5432/postgres")   // URL PostgreSQL di dalam Docker Network. postgres -> nama service pada docker-compose.
                    .withDriverName("org.postgresql.Driver")       // Driver PostgreSQL
                    .withUsername("postgres")   // Username PostgreSQL.
                    .withPassword("postgres")   // Password PostgreSQL.
                    .build()        // Build object koneksi.
            )
        );


        // Menjalankan Flink Job
        
        /*
            Seluruh pipeline yang telah dibuat sebelumnya BELUM berjalan.
            Semua baris sebelumnya hanya mendefinisikan pipeline.
            Pipeline benar-benar mulai berjalan setelah env.execute() dipanggil.
        */
        env.execute("Kafka-flink-postgres");      // Nama Job. Nama ini akan muncul pada Flink Dashboard.
    }
                
                
    // AverageAggregator
    // Class ini bertugas menghitung rata-rata temperature untuk setiap kota (city).
    // Class ini akan dipanggil oleh .aggregate(new AverageAggregator()) pada pipeline DataStream.
    public static class AverageAggregator implements AggregateFunction<
        Weather,                    // Input
        MyAverage,                  // Accumulator
        Tuple2<MyAverage, Double>   // Output
    > {

        // createAccumulator
        // Dipanggil pertama kali ketika Flink membuat sebuah aggregator baru.
        // Flink membutuhkan sebuah object untuk menyimpan proses perhitungan sementara. Object inilah yang disebut Accumulator
        @Override
        public MyAverage createAccumulator() {
            // Membuat object MyAverage baru.
            /*
            Nilai awal:
                city = null
                count = 0
                sum = 0
            */
            return new MyAverage();
        }

        // add() -> Method ini dipanggil setiap ada data baru.
        /*
            Misalnya producer mengirim:
                Jakarta 30
                Jakarta 35
                Jakarta 40
            Maka add() dipanggil sebanyak 3 kali.
        */
        @Override
        public MyAverage add(Weather weather, MyAverage myAverage) {
            myAverage.city = weather.city;      // Variable nama kota.

            // Tambah jumlah data.
            /*
                Misalnya:
                    sebelumnya
                    count = 2
                    maka menjadi
                    count = 3
            */
            myAverage.count = myAverage.count + 1;

            // Tambahkan temperature ke total sementara.
            /*
                Misalnya:
                sum = 60
                data baru = 35
            
                hasil:
                sum = 95
             */
            myAverage.sum = myAverage.sum + weather.temperature;    
            return myAverage;       // Return accumulator yang telah diperbarui.
        }

        // getResult() -> Dipanggil ketika Window selesai.
        /*
            Misalnya:
            Window:
                Jakarta
                30
                40
                50
            Sum = 120
            Count = 3
            Average = 40
        */
        @Override
        public Tuple2<MyAverage, Double> getResult(MyAverage myAverage) {
            /*
            Return:
                Tuple2
                f0 = accumulator
                f1 = average
             */
            return new Tuple2<>(myAverage, myAverage.sum / myAverage.count);
        }


        // merge() -> Digunakan ketika Flink perlu menggabungkan dua accumulator.
        // Biasanya terjadi pada Session Window / Parallel Processing.
        // Pada project ini sebenarnya hampir tidak pernah dipanggil, tetapi wajib diimplementasikan karena AggregateFunction mengharuskannya.
        @Override
        public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
            myAverage.sum = myAverage.sum + acc1.sum;           // Gabungkan total sum.
            myAverage.count = myAverage.count + acc1.count;     // Gabungkan jumlah data.
            return myAverage;                                   // Return accumulator gabungan.
        }
    }

    // Class MyAverage
    // Class ini merupakan Accumulator yang digunakan oleh AverageAggregator.
    // Flink akan menyimpan object ini selama proses aggregate.
    // Isi object akan terus berubah setiap ada data baru yang masuk.
    public static class MyAverage {
        public String city;         // Variable untuk nama kota
        public Integer count = 0;   // Jumlah data yang telah diterima. Setiap ada event baru, nilainya akan bertambah 1.
        public Double sum = 0d;     // Total temperature sementara. Setiap ada event baru, nilainya akan ditambah temperature event tersebut.

        // toString() -> Digunakan ketika object dicetak menggunakan System.out.println() atau DataStream.print()
        @Override
        public String toString() {
            return "MyAverage{" +
                    "city='" + city + '\'' +    // Menampilkan nama kota.
                    ", count=" + count +        // Menampilkan jumlah data.
                    ", sum=" + sum +           // Menampilkan total temperature. 
                    '}';            // Penutup object.
        }
    }
}
