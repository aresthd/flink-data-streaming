// Import utility untuk membuat hashCode
import java.util.Objects;

// Class Weather
public class Weather {

    /*
    Contoh data JSON dari Kafka
    {
        "city":"New York",
        "temperature":10.34
    }
    */
    
    public String city;         // Variable untuk menyimpan nama kota
    public Double temperature;  // Variable untuk menyimpan Temperatur

    // Constructor kosong
    // WAJIB untuk Jackson Deserializer
    public Weather() {
    }

    // Constructor dengan parameter
    public Weather(String city, Double temperature) {
        this.city = city;                               // Isi field city
        this.temperature = Double.valueOf(temperature); // Isi field temperature
    }

    // Override method toString()
    // Agar object bisa dicetak dengan mudah
    @Override
    public String toString() {
        // String builder lebih efisien
        final StringBuilder sb = new StringBuilder("Weather{");
        
        // Tambahkan field city
        sb.append("city='")
          .append(city)
          .append('\'');

        // Tambahkan field temperature
        sb.append(", temperature=")
          .append(String.valueOf(temperature))
          .append('\'');

        // Return string hasil build
        return sb.toString();
    }

    // Digunakan untuk hashing
    public int hashCode() {
        // Gunakan Objects.hash() untuk membuat hashCode dari field city dan temperature
        return Objects.hash(
            super.hashCode(),
            city,
            temperature
        );
    }
}