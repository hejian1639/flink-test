package flink.test.broadcast;

import flink.test.wordcount.util.WordCountData;
import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Desc: 广播变量，定时从数据库读取告警规则数据
 * Created by zhisheng on 2019-05-30
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {


    @AllArgsConstructor
    static class Event {
        String s;
    }
    final static MapStateDescriptor<String, Event> ALARM_RULES = new MapStateDescriptor<>(
            "alarm_rules",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeExtractor.createTypeInfo(Event.class));

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> alarmDataStream = env.fromElements(WordCountData.WORDS).flatMap(new Tokenizer());//数据流定时从数据库中查出来数据



        SingleOutputStreamOperator<Event> metricEventDataStream = env.fromElements(new String[]{"to", "be"}).map(Event::new);


        alarmDataStream.connect(metricEventDataStream.broadcast(ALARM_RULES))
                .process(new BroadcastProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, Event> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                        if (broadcastState.contains(value)) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        System.out.println("thread " + Thread.currentThread().getId());
                        BroadcastState<String, Event> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                        broadcastState.put(value.s, value);
                    }
                }).setParallelism(1).print();


        env.execute("zhisheng broadcast demo");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) throws InterruptedException {
            System.out.println(Thread.currentThread().getId());
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(token);
                    Thread.sleep(10);
                }
            }
        }
    }
}
