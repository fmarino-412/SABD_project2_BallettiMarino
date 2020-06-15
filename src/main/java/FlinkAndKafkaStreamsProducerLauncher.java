import kafka_pubsub.KafkaSingleProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

@SuppressWarnings("BusyWait")
public class FlinkAndKafkaStreamsProducerLauncher {

	private static final String CSV_PATH = "data/dataset.csv";
	private static final Long SLEEP = 10L;

	public static void main(String[] args) {

		KafkaSingleProducer producer = new KafkaSingleProducer();
		String line;
		Long eventTime;

		try {
			FileReader file = new FileReader(CSV_PATH);
			BufferedReader bufferedReader = new BufferedReader(file);

			while ((line = bufferedReader.readLine()) != null) {
				try {
					String[] info = line.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
					format.setTimeZone(TimeZone.getTimeZone("America/New_York"));
					eventTime = format.parse(info[7]).getTime();
					producer.produce(eventTime, line, eventTime);
					Thread.sleep(SLEEP);
				} catch (ParseException | InterruptedException ignored) {
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
