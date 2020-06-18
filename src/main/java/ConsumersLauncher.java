import kafka_pubsub.KafkaClusterConfig;
import kafka_pubsub.ProcessingConsumers;
import utility.OutputFormatter;

public class ConsumersLauncher {
	public static void main(String[] args) {

		int id = 0;
		for (int i = 0; i < KafkaClusterConfig.FLINK_TOPICS.length; i++) {
			new Thread(new ProcessingConsumers(id,
					KafkaClusterConfig.FLINK_TOPICS[i],
					true,
					OutputFormatter.FLINK_OUTPUT_FILES[i]));
		}
		for (String topic : KafkaClusterConfig.KAFKA_TOPICS) {
			new Thread(new ProcessingConsumers(id, topic, false, null));
		}
	}
}
