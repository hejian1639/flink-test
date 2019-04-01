package flink.test;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;
import java.util.Set;

public class UniqueCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> dataStream = env
                .socketTextStream("localhost", 9999)
                .timeWindowAll(Time.seconds(5))
                .aggregate(new AggregateFunction<String, Set<String>, Integer>() {


                    @Override
                    public Set<String> createAccumulator() {
                        return new HashSet<>();
                    }

                    @Override
                    public Set<String> add(String value, Set<String> accumulator) {
                        accumulator.add(value);
                        return accumulator;
                    }

                    @Override
                    public Integer getResult(Set<String> accumulator) {
                        return accumulator.size();
                    }

                    @Override
                    public Set<String> merge(Set<String> a, Set<String> b) {
                        a.addAll(b);
                        return a;
                    }
                });

//        env.socketTextStream("localhost", 9999)
//                .flatMap((FlatMapFunction<String, Tuple2<Integer, String>>) (word, out) ->
//                        out.collect(new Tuple2<>(1, word))
//                ).returns(Types.TUPLE(Types.INT, Types.STRING))
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//                .aggregate(new AggregateFunction<Tuple2<Integer, String>, Set<String>, Integer>() {
//
//
//                    @Override
//                    public Set<String> createAccumulator() {
//                        return new HashSet<>();
//                    }
//
//                    @Override
//                    public Set<String> add(Tuple2<Integer, String> value, Set<String> accumulator) {
//                        accumulator.add(value.f1);
//                        return accumulator;
//                    }
//
//                    @Override
//                    public Integer getResult(Set<String> accumulator) {
//                        return accumulator.size();
//                    }
//
//                    @Override
//                    public Set<String> merge(Set<String> a, Set<String> b) {
//                        a.addAll(b);
//                        return a;
//                    }
//                }).print();


        dataStream.print();
//        dataStream.addSink(new PrintSinkFunction<>()).name("Print to Std. Out");

        env.execute("Window WordCount");
    }


}