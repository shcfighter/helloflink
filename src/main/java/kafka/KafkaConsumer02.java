package kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaConsumer02 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "111.231.132.168:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("log-test", new SimpleStringSchema(), properties);
        //从最早开始消费
        //consumer.setStartFromEarliest();
        DataStream<String> stream = env
                .addSource(consumer);

        stream.print();
        DataStream<Tuple2<String, Integer>> result = stream.flatMap(new Tokenizer())
                .keyBy(0)
                .timeWindow(Time.seconds(60))
                //.sum(1);
                .reduce(new ReduceFun());
        //result.writeAsCsv("/data/kafka.txt", FileSystem.WriteMode.OVERWRITE);
        result.print();
        env.execute("kafka_window_wordcount");

    }

    public static class ReduceFun implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
            return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
        }
    }

    // User-defined functions
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}