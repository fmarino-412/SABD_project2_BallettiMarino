import kafka_pubsub.KafkaClusterConfig;
import kafka_pubsub.KafkaParametricConsumer;
import utility.OutputFormatter;

import java.util.ArrayList;
import java.util.Scanner;

/**
 * Class used to launch consumers for Flink and Kafka Streams output
 */
public class ConsumersLauncher {

	public static void main(String[] args) {

		//cleaning result directory to store data results
		OutputFormatter.cleanResultsFolder();
		System.out.println("Result directory prepared");

		// create a consumer structure to allow stopping
		ArrayList<KafkaParametricConsumer> consumers = new ArrayList<>();

		int id = 0;
		// launch Flink topics consumers
		for (int i = 0; i < KafkaClusterConfig.FLINK_TOPICS.length; i++) {
			KafkaParametricConsumer consumer = new KafkaParametricConsumer(id,
					KafkaClusterConfig.FLINK_TOPICS[i],
					true,
					OutputFormatter.FLINK_OUTPUT_FILES[i]);
			consumers.add(consumer);
			new Thread(consumer).start();
			id++;
		}
		// launch Kafka Streams topics consumers
		for (String topic : KafkaClusterConfig.KAFKA_TOPICS) {
			KafkaParametricConsumer consumer = new KafkaParametricConsumer(id, topic, false, null);
			consumers.add(consumer);
			new Thread(consumer).start();
			id++;
		}

		System.out.println("\u001B[36m" + "Enter something to stop consumers" + "\u001B[0m");
		Scanner scanner = new Scanner(System.in);
		// wait for the user to digit something
		scanner.next();
		System.out.println("Sending shutdown signal to consumers");
		// stop consumers
		for (KafkaParametricConsumer consumer : consumers) {
			consumer.stop();
		}
		System.out.flush();
		System.out.println("Signal sent, closing...");
	}
}
