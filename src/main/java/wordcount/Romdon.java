package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Romdon {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSet = environment.fromElements("5","3","2","4","2","5","31","3","56","6","43");
        dataSet.print();
        DataSet<Tuple2<String, Integer>> dataSet2 = dataSet.map(new MapFun()).groupBy(0).sum(1);

        dataSet2.print();

        environment.execute();
    }

    public static class MapFun implements MapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String integer) throws Exception {
            return new Tuple2(integer, 1);
        }
    }

}
