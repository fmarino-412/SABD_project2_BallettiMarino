import kafka_pubsub.KafkaSingleProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Class used to start a producer that reads from file and send tuples to Kafka topics
 */
@SuppressWarnings("BusyWait")
public class ProducerLauncher {

	private static final String CSV_PATH = "data/dataset.csv";
	private static final Long SLEEP = 10L;

	public static void main(String[] args) {

		// create producer
		KafkaSingleProducer producer = new KafkaSingleProducer();

		String line;
		Long eventTime;

		try {
			// read from file
			FileReader file = new FileReader(CSV_PATH);
			BufferedReader bufferedReader = new BufferedReader(file);

			while ((line = bufferedReader.readLine()) != null) {
				try {
					// produce tuples simulating a data stream processing source
					String[] info = line.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
					eventTime = format.parse(info[7]).getTime();
					producer.produce(eventTime, line, eventTime);
					// for real data stream processing source simulation
					//Thread.sleep(SLEEP);
				} catch (ParseException /*| InterruptedException*/ ignored) {
				}
			}

			bufferedReader.close();
			file.close();
			producer.close();
			System.out.println("Producer process completed");
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
}
