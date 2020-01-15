package flink.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SplitUdpWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> text = env
                .addSource(new UdpStreamFunction(8001))
                .map(word -> new Tuple2<>(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SplitStream<Tuple2<String, Integer>> step = text.split(new Selector());

        step.select("even").addSink(new PrintSinkFunction()).slotSharingGroup("1");

        step.select("odd").addSink(new PrintSinkFunction()).slotSharingGroup("2");


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