import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

public class WeatherDeserializationSchema extends AbstractDeserializationSchema<Weather> {
    private static final long serialVersionUUID = 1L;

    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    @Override
    public Weather deserialize(byte[] message) throws IOException {
        String json = new String(message);
        System.out.println("Kafka JSON : " + json);

        Weather weather = objectMapper.readValue(message, Weather.class);
        System.out.println("Parsed : " + weather);

        // return objectMapper.readValue(message, Weather.class);
        return weather;
    }
}
