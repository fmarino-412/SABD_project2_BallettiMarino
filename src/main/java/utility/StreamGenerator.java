package utility;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class StreamGenerator implements SourceFunction<String> {

    private Boolean isRunning = true;
    private static final Long SLEEP = 10L;
    private static final String FILEPATH = "data/dataset.csv";


    public void run(SourceContext<String> sourceContext) throws Exception {

        FileReader fileReader = new FileReader(FILEPATH);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        BusData data;
        String line;

        while (isRunning && (line = bufferedReader.readLine()) != null) {
            sourceContext.collect(line);
            Thread.sleep(SLEEP);
        }
    }

    public void cancel() {
        isRunning = false;
    }
}
