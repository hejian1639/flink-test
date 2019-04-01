package flink.npv;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

public class Experiernce {


    public static void main(String[] args) throws Exception {


        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().disableSysoutLogging();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<NpvHttp> source = env.fromCollection(Arrays.asList(NpvHttp.of(1, "1", 2001),
                NpvHttp.of(2, "1", 2001), NpvHttp.of(3, "1", 2000)));


        source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<NpvHttp>() {
            @Override
            public long extractAscendingTimestamp(NpvHttp element) {
                return element.getTimestamp();
            }
        }).timeWindowAll(Time.seconds(5))
                .aggregate(new AggregateFunction<NpvHttp, Metrics, Metrics>() {

                    MapReducer mapReducer = new MapReducer();

                    @Override
                    public Metrics createAccumulator() {
                        return new Metrics();
                    }

                    @Override
                    public Metrics add(NpvHttp value, Metrics accumulator) {
                        Metrics m = mapReducer.map(value);
                        return mapReducer.reduce(accumulator, m);
                    }

                    @Override
                    public Metrics getResult(Metrics accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Metrics merge(Metrics a, Metrics b) {
                        return mapReducer.reduce(a, b);
                    }
                })
                .map(m -> m.total)
                .print();

        env.execute("output");

        // get input data
//        DataSet<NpvHttp> source = env.fromElements(NpvHttp.of("1", 2001), NpvHttp.of("1", 2001), NpvHttp.of("1", 2000));
//
//        // split up the lines in pairs (2-tuples) containing: (word,1)
//        DataSet<Double> exp = source
//                .map(npv -> {
//                    Metrics metrics=new Metrics();
//
//                    if(npv.rspDelayTime>2000){
//                        metrics.good=0;
//                    }
//
//                    metrics.total=1;
//
//                    return metrics;
//                }).reduce((a, b)->{
//                    a.good+=b.good;
//                    a.total+=b.total;
//
//                    return a;
//                }).map( m -> (Double.valueOf(m.good))/m.total);


//        DataSet<Double> exp = total.join(good)
//                .where(0)
//                .equalTo(0)
//                .map(t -> (Double.valueOf(t.f1.f1))/t.f0.f1);


//        exp.print();

//        Stream<NpvHttp> source = Stream.source(NpvHttp.of("1", 0), NpvHttp.of("1", 0), NpvHttp.of("2", 0)).window($1mins);
//
//
//        source.filter(npv->npv.rspDelayTime<2000).count();
//        source.count();
    }


}