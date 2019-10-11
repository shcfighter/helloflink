package kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaConsumerLog {

    private static String pattern = "\\[([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}:[0-9]{3})\\]\\s\\[([A-Z]*)\\]\\s\\[([\\w\\.-]*)\\]\\s\\[([\\w\\.():]*)\\]\\s-\\s([\\s\\S]*)";

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "111.231.132.168:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log-test", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();
        DataStream<String> stream = env
                .addSource(consumer);

        stream.print();

        DataStream<Tuple5<String, String, String, String, String>> result = stream.map(new Tokenizer());

        result.writeAsCsv("/data/kafka.txt", FileSystem.WriteMode.OVERWRITE);
        result.print();
        env.execute("kafka_window_log");

    }

    // User-defined functions
    public static class Tokenizer implements MapFunction<String, Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> map(String s) throws Exception {
            Pattern r = Pattern.compile(pattern);
            Matcher matcher = r.matcher(s);
            boolean match = matcher.matches();
            if (match) {
                return Tuple5.of(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4), matcher.group(5));
            }
            return Tuple5.of("", "", "", "", "");
        }
    }
}