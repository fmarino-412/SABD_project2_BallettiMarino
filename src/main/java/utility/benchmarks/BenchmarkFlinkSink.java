package utility.benchmarks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * .addSink(new BenchmarkFlinkSink())
 */
public class BenchmarkFlinkSink implements SinkFunction<String> {

	@Override
	public void invoke(String value, Context context) {
		SynchronizedCounter.incrementCounter();
	}
}
