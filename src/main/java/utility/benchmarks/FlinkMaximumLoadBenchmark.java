package utility.benchmarks;

import flink_dsp.query1.Query1TopologyBuilder;
import flink_dsp.query2.Query2TopologyBuilder;
import flink_dsp.query3.Query3TopologyBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import scala.Tuple2;
import utility.OutputFormatter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class FlinkMaximumLoadBenchmark {

	final static int VALID_TUPLE_NUMBER = 379412;
	final static int RUNS = 10;

	public static void main(String[] args) {

		List<Long> times = new ArrayList<>();
		Long current;

		for (int i = 0; i < RUNS; i++) {
			current = executeRun();
			if (current >= 0L) {
				times.add(current);
			}
		}

		double meanTime = times.stream().mapToDouble(a -> a).average().orElse(0.0);
		System.out.println("\u001B[36m" +
				"Evaluated mean throughput: " + VALID_TUPLE_NUMBER / meanTime + " tuples/millisecond" +
				"\nMean time: " + meanTime + " milliseconds.\u001B[0m");
	}

	public static Long executeRun() {


		// Cleaning result directory to store data results
		OutputFormatter.cleanResultsFolder();

		// Setup flink environment
		Configuration conf = new Configuration();
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<Long, String>> source = environment.addSource(

				new SourceFunction<Tuple2<Long, String>>() {

					private Boolean isRunning = true;

					@Override
					public void run(SourceContext<Tuple2<Long, String>> sourceContext) throws Exception {

						FileReader fileReader = new FileReader("data/dataset.csv");
						BufferedReader bufferedReader = new BufferedReader(fileReader);
						String line;

						while (isRunning && (line = bufferedReader.readLine()) != null) {
							String[] info = line.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
							DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
							try {
								sourceContext.collect(new Tuple2<>(format.parse(info[7]).getTime(), line));
							} catch (ParseException ignored) {

							}
						}
					}

					@Override
					public void cancel() {
						isRunning = false;
					}
				}).name("stream-source")
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Tuple2<Long, String>>() {
							@Override
							public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {
								return tuple._1();
							}
						}
				);

		Query1TopologyBuilder.buildTopology(source);
		Query2TopologyBuilder.buildTopology(source);
		Query3TopologyBuilder.buildTopology(source);

		try {
			long startTime = System.currentTimeMillis();
			environment.execute();
			return System.currentTimeMillis() - startTime;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return -1L;

	}
}
