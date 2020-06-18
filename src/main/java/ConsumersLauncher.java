import kafka_pubsub.KafkaClusterConfig;
import kafka_pubsub.KafkaParametricConsumer;
import utility.OutputFormatter;

import java.util.ArrayList;
import java.util.Scanner;

public class ConsumersLauncher {

	public static void main(String[] args) {

		ArrayList<KafkaParametricConsumer> consumers = new ArrayList<>();

		int id = 0;
		for (int i = 0; i < KafkaClusterConfig.FLINK_TOPICS.length; i++) {
			KafkaParametricConsumer consumer = new KafkaParametricConsumer(id,
					KafkaClusterConfig.FLINK_TOPICS[i],
					true,
					OutputFormatter.FLINK_OUTPUT_FILES[i]);
			consumers.add(consumer);
			new Thread(consumer).start();
			id++;
		}
		for (String topic : KafkaClusterConfig.KAFKA_TOPICS) {
			KafkaParametricConsumer consumer = new KafkaParametricConsumer(id, topic, false, null);
			consumers.add(consumer);
			new Thread(consumer).start();
			id++;
		}

		System.out.println("\u001B[36m" + "Enter something to stop consumers" + "\u001B[0m");
		Scanner scanner = new Scanner(System.in);
		scanner.next();
		System.out.println("Sending shutdown signal to consumers");
		for (KafkaParametricConsumer consumer : consumers) {
			consumer.stop();
		}
		System.out.flush();
		System.out.println("Signal sent, closing...");
	}
}
