package flink.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

@Slf4j
public class HttpWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .addSource(new HttpStreamFunction(8000))
                .map(word -> new Tuple2<>(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

//        dataStream.print();
        dataStream.addSink(new PrintSinkFunction<>()).name("Print to Std1. Out");
        dataStream.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            public void invoke(Tuple2<String, Integer> value, Context context) {
                log.info("{}", value);
            }

        }).name("Print to Std2. Out");

        env.execute("Window WordCount");
    }


}