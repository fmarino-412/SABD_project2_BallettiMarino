package kafkastreams_dsp.query3;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import utility.BusData;
import utility.delay_parsing.DelayFormatException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Locale;

public class Query3TopologyBuilder {

    public static void buildTopology(KStream<Long, String> source) {

        KStream<Long, BusData> preprocessed = source.flatMapValues(new ValueMapper<String, Iterable<? extends BusData>>() {
            @Override
            public Iterable<? extends BusData> apply(String s) {
                ArrayList<BusData> result = new ArrayList<>();
                String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                try {
                    result.add(new BusData(info[7], info[11], info[5], info[10]));
                } catch (ParseException | DelayFormatException | NumberFormatException ignored) {

                }
                return result;
            }
        });

        // 1 day statistics
        preprocessed.map(new KeyValueMapper<Long, BusData, KeyValue<String, BusData>>() {
            @Override
            public KeyValue<String, BusData> apply(Long aLong, BusData busData) {
                StringBuilder newKey = new StringBuilder();

                Calendar calendar = Calendar.getInstance(Locale.US);
                return null;
            }
        });
        // 7 day statistics
    }
}
