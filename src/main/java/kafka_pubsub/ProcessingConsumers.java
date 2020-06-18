package kafka_pubsub;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

public class ProcessingConsumers implements Runnable {

	private final static boolean PRINT_KAFKA_ON_CONSOLE = false;
	private final static int INTERVAL_TIME = 1000;
	private String CONSUMER_GROUP_ID = "-topics-consumers";
	private Consumer<String, String> consumer;
	private int id;
	private String topic;
	private boolean flink;
	private String path;

	private Consumer<String, String> createConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class.getName());

		return new KafkaConsumer<>(props);
	}

	private static void subscribeToTopic(Consumer<String, String> consumer, String topic) {
		consumer.subscribe(Collections.singletonList(topic));
	}

	public ProcessingConsumers(int id, String topic, boolean flink, @Nullable String path) {
		this.flink = flink;
		if (flink) {
			CONSUMER_GROUP_ID = "flink" + CONSUMER_GROUP_ID;
			this.path = Objects.requireNonNull(path);
		} else {
			CONSUMER_GROUP_ID = "kafka-streams" + CONSUMER_GROUP_ID;
		}

		this.id = id;
		this.topic = topic;
		consumer = createConsumer();
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

	@SuppressWarnings("BusyWait")
	private void runKafkaStreamsConsumer() {
		System.out.println("Kafka Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " running...");
		try {
			while (true) {
				Thread.sleep(INTERVAL_TIME);
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(INTERVAL_TIME));
				if (PRINT_KAFKA_ON_CONSOLE) {
					for (ConsumerRecord<String, String> record : records) {
						System.out.println("[" + id + "] Consuming Kafka record at topic: " + topic +
								"\n(key=" + record.key() + ", val=" + record.value() + ")");
					}
				}
			}
		} catch (InterruptedException ignored) {

		} finally {
			consumer.close();
		}
	}

	@SuppressWarnings({"BusyWait", "ResultOfMethodCallIgnored"})
	private void runFlinkConsumer() {
		System.out.println("Flink Consumer " + CONSUMER_GROUP_ID + "-ID" + id + " running...");
		try {
			while (true) {
				Thread.sleep(INTERVAL_TIME);
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(INTERVAL_TIME));

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
					}

					bw.close();
					writer.close();
				}
			}

		} catch (InterruptedException ignored) {

		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not export result to " + path);
		}finally {
			consumer.close();
		}
	}
}
