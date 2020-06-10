package utility;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utility.delay_parsing.DelayFormatException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;

public class StreamGenerator implements SourceFunction<BusData> {

    private Boolean isRunning = true;
    private static final Long SLEEP = 10L;
    private static final String FILEPATH = "data/dataset.csv";


    public void run(SourceContext<BusData> sourceContext) throws Exception {

        FileReader fileReader = new FileReader(FILEPATH);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        BusData data;
        String line;

        while (isRunning && (line = bufferedReader.readLine()) != null) {
            String[] info = line.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            try {
                data = new BusData(info[7], info[11], info[9]);
                sourceContext.collect(data);
                Thread.sleep(SLEEP);
            } catch (ParseException| DelayFormatException |NumberFormatException ignored) {
                // ignore and skip to next line
                /*
                if (!e.getMessage().equals("Could not find any delay information in string: ")) {
                    System.err.println(e.getMessage());
                }
                */
            }
        }
    }

    public void cancel() {
        isRunning = false;
    }
}
