package utility.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class SerDesBuilders {
	public static <T> Serde<T> getSerdes(Class<T> classT) {
		Map<String, Object> serdeProps = new HashMap<>();

		Serializer<T> serializer = new JsonPOJOSerializer<>();
		Deserializer<T> deserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", classT);
		serializer.configure(serdeProps, false);
		serdeProps.put("JsonPOJOClass", classT);
		deserializer.configure(serdeProps, false);

		return Serdes.serdeFrom(serializer, deserializer);
	}
}
