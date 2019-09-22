package flink.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UdpWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> text = env
                .addSource(new UdpStreamFunction(8001))
                .map(word -> new Tuple2<>(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SplitStream<Tuple2<String, Integer>> step = text.split(new Selector());

        step.select("even").keyBy(0).reduce((value1, value2) -> {
            System.out.println("even thread " + Thread.currentThread().getId());
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }).print();
        step.select("odd").keyBy(0).reduce((value1, value2) -> {
            System.out.println("odd thread " + Thread.currentThread().getId());
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }).print();


        env.execute("Window WordCount");
    }

    /**
     * OutputSelector testing which tuple needs to be iterated again.
     */
     static class Selector implements OutputSelector<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> select(Tuple2<String, Integer> value) {
            List<String> output = new ArrayList<>();
            if (value.f0.charAt(0) % 2 == 0) {
                output.add("even");
            } else {
                output.add("odd");

            }
            return output;
        }
    }

}