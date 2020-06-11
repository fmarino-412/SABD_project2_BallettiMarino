package utility;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class StreamGenerator implements SourceFunction<String> {

    private Boolean isRunning = true;
    private static final Long SLEEP = 10L;
    private static final String FILEPATH = "data/dataset.csv";
    private static final Long MAX_FILE_UPDATE_WAIT_SECONDS = 10L;


    public void run(SourceContext<String> sourceContext) throws Exception {

        FileTailReader reader = new FileTailReader(new File(FILEPATH), MAX_FILE_UPDATE_WAIT_SECONDS);
        reader.start();

        while (isRunning && !reader.hasEnded()) {
            while (reader.linesAvailable()) {
                sourceContext.collect(reader.getLine());
                // to simulate a data source
                Thread.sleep(SLEEP);
            }
            // wait for new data
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        isRunning = false;
    }
}
