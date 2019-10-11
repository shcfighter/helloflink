package dataset;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class Join {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //env.readCsvFile("d://1.csv").pojoType(Student.class, "id", "name", "age");
        DataSet<Tuple3<Integer, String, String>> studentSet = env.readCsvFile("d://1.csv").fieldDelimiter(",")
                .includeFields(true,true,true)
                .types(Integer.class, String.class, String.class);
        studentSet.print();

        DataSet<Tuple3<Integer, String, Integer>> courseSet = env.readCsvFile("d://2.csv").fieldDelimiter(",")
                .includeFields(true,true,true)
                .types(Integer.class, String.class, Integer.class);
        courseSet.print();
        //env.execute();

        JoinFunctionAssigner<Tuple3<Integer, String, String>, Tuple3<Integer, String, Integer>> joinFunctionAssigner = studentSet.leftOuterJoin(courseSet).where(0).equalTo(0);
        joinFunctionAssigner.with(new FlatJoinFunction<Tuple3<Integer, String, String>, Tuple3<Integer, String, Integer>, Tuple6<Integer, String, String, Integer, String, Integer>>() {
            @Override
            public void join(Tuple3<Integer, String, String> t1, Tuple3<Integer, String, Integer> t2, Collector<Tuple6<Integer, String, String, Integer, String, Integer>> collector) throws Exception {
                if (Objects.nonNull(t2)) {
                    collector.collect(Tuple6.of(t1.f0, t1.f1, t1.f2, t2.f0, t2.f1, t2.f2));
                } else {
                    collector.collect(Tuple6.of(t1.f0, t1.f1, t1.f2, 0, "", 0));
                }
            }
        }).print();

        System.out.println("=================================================================================");

        joinFunctionAssigner.with(new JoinFunction<Tuple3<Integer, String, String>, Tuple3<Integer, String, Integer>, Tuple6<Integer, String, String, Integer, String, Integer>>() {
            @Override
            public Tuple6<Integer, String, String, Integer, String, Integer> join(Tuple3<Integer, String, String> t1, Tuple3<Integer, String, Integer> t2) throws Exception {
                if (Objects.nonNull(t2)) {
                    return Tuple6.of(t1.f0, t1.f1, t1.f2, t2.f0, t2.f1, t2.f2);
                } else {
                    return Tuple6.of(t1.f0, t1.f1, t1.f2, 0, "", 0);
                }
            }
        }).print();
    }
}
