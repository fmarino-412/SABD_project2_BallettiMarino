package utility.benchmarks;

public class SynchronizedCounter {

	private static long counter = 0L;
	private static long startTime;

	public static synchronized void incrementCounter() {
		if (counter == 0L) {
			startTime = System.currentTimeMillis();
			System.out.println("Initialized!");
		}
		counter++;
		double currentTime = System.currentTimeMillis() - startTime;
		System.out.println("Mean throughput: " + (counter/currentTime) + "\n" + "Mean latency: " +
				(currentTime/counter));
	}
}
