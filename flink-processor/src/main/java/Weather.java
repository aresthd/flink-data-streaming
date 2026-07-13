import java.util.Objects;

public class Weather {

    /* 
    {
        "city"}: "New York",
        "temperature": "10.34"
    }
    */

    public String city;
    public Double temperature;

    // WAJIB
    public Weather() {
    }

    public Weather(String city, Double temperature) {
        this.city = city;
        this.temperature = Double.valueOf(temperature);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Weather{");
        sb.append("city='").append(city).append('\'');
        sb.append(", temperature=").append(String.valueOf(temperature)).append('\'');
        // sb.append('}');
        return sb.toString();
    }

    public int hashCode() {
        return Objects.hash(super.hashCode(), city, temperature);
    }

    
    
}
