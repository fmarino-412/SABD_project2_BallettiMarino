package kafka_pubsub;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

/**
 * Class used to create consumers, subscribe them to topics and run them
 */
public class KafkaParametricConsumer implements Runnable {
	private final static boolean PRINT_KAFKA_ON_CONSOLE = true;
	private final static int POLL_WAIT_TIME = 1000;
	private final static int CYCLE_INTERVAL_TIME = 5000;
	private String CONSUMER_GROUP_ID = "-topics-consumers";
	private final Consumer<String, String> consumer;
	private final int id;
	private final String topic;
	private final boolean flink;
	private String path;
	private boolean running = true;

	/**
	 * Create a new consumer using the properties
	 * @return the created consumer
	 */
	private Consumer<String, String> createConsumer() {
		Properties props = KafkaClusterConfig.getKafkaParametricConsumerProperties(CONSUMER_GROUP_ID);
		return new KafkaConsumer<>(props);
	}

	/**
	 * Subscribe a consumer to a topic
	 * @param consumer to be subscribe
	 * @param topic chosen
	 */
	private static void subscribeToTopic(Consumer<String, String> consumer, String topic) {
		consumer.subscribe(Collections.singletonList(topic));
	}

	/**
	 * Create a parametric consumer based on the arguments
	 * @param id consumer's id
	 * @param topic name
	 * @param flink boolean, if true is a flink consumer, in the other case it's a kafka one
	 * @param path where to store result if it's a flink consumer, null if it isn't
	 */
	public KafkaParametricConsumer(int id, String topic, boolean flink, @Nullable String path) {
		this.flink = flink;
		if (flink) {
			CONSUMER_GROUP_ID = "flink" + CONSUMER_GROUP_ID;
			this.path = Objects.requireNonNull(path);
		} else {
			CONSUMER_GROUP_ID = "kafka-streams" + CONSUMER_GROUP_ID;
		}

		this.id = id;
		this.topic = topic;

		// create the consumer
		consumer = createConsumer();

		// subscribe the consumer to the topic
		subscribeToTopic(consumer, topic);
	}

	@Override
	public void run() {
		if (flink) {
			runFlinkConsumer();
		} else {
			runKafkaStreamsConsumer();
		}
	}

	/**
	 * Create a consumer that prints kafka streams queries' result if PRINT_KAFKA_ON_CONSOLE is set to true
	 */
	@SuppressWarnings({"BusyWait"})
	private void runKafkaStreamsConsumer() {
		System.out.println("Kafka Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " running...");
		try {
			while (running) {
				Thread.sleep(CYCLE_INTERVAL_TIME);
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_WAIT_TIME));
				if (PRINT_KAFKA_ON_CONSOLE) {
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("[" + id + "] Consuming Kafka record at topic: " + topic +
								"\n(key=" + record.key() + ", val=" + record.value() + ")");
					}
				}
			}
		} catch (InterruptedException ignored) {
			// ignored
		} finally {
			// close consumer
			consumer.close();
			System.out.println("Kafka Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " stopped");
		}
	}

	/**
	 * Create a consumer that write flink queries' result to the csv in Results directory
	 */
	@SuppressWarnings({"BusyWait", "ResultOfMethodCallIgnored"})
	private void runFlinkConsumer() {
		System.out.println("Flink Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " running...");
		try {
			while (running) {
				Thread.sleep(CYCLE_INTERVAL_TIME);
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_WAIT_TIME));

				if (!records.isEmpty()) {
					File file = new File(path);
					if (!file.exists()) {
						// creates the file if it does not exist
						file.createNewFile();
					}

					// append to existing version of the same file
					FileWriter writer = new FileWriter(file, true);
					BufferedWriter bw = new BufferedWriter(writer);

					for (ConsumerRecord<String, String> record : records) {
						bw.append(record.value());
						bw.append("\n");
					}

					// close both buffered writer and file writer
					bw.close();
					writer.close();
				}
			}

		} catch (InterruptedException ignored) {
			// ignored
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not export result to " + path);
		} finally {
			// close consumer
			consumer.close();
			System.out.println("Flink Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " stopped");
		}
	}

	public void stop() {
		this.running = false;
	}
}
