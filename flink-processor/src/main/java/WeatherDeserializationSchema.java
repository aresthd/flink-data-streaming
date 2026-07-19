import com.fasterxml.jackson.databind.ObjectMapper;                                 // Library Jackson untuk parsing JSON
import com.fasterxml.jackson.databind.json.JsonMapper;                              // Builder ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;                        // Support Java Time
import java.io.IOException;                                                         // IOException
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;     // Parent class Flink deserializer

// Deserializer menghasilkan object Weather
// Class WeatherDeserializationSchema yang berfungsi untuk mengubah data JSON dari Kafka menjadi object Weather
public class WeatherDeserializationSchema extends AbstractDeserializationSchema<Weather> {
    private static final long serialVersionUID = 1L;    // Variable untuk menghindari warning serialVersionUID
    private transient ObjectMapper objectMapper;        // Variable untuk Jackson ObjectMapper, transient agar tidak diserialisasi

    // Dipanggil saat Flink memulai operator
    @Override
    public void open(InitializationContext context) {
        // Membuat object mapper
        objectMapper =
            JsonMapper.builder()
                .build()
                .registerModule(
                    new JavaTimeModule()
                );
    }

    // Dipanggil setiap message Kafka datang
    @Override
    public Weather deserialize(
        byte[] message
    ) throws IOException {

        // Convert byte[] → String
        String json = new String(message);

        System.out.println(
            "Kafka JSON : " + json
        );

        // Parse JSON → Weather
        Weather weather =
            objectMapper.readValue(
                message,
                Weather.class
            );

        System.out.println(
            "Parsed : " + weather
        );

        // return objectMapper.readValue(message, Weather.class);
        return weather;
    }
}
