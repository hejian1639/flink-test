package flink.npv;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

import java.util.HashSet;

public class HttpOnline {


    public static void main(String[] args) throws Exception {


//        // set up the execution environment
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//
//        // get input data
//        DataSet<NpvHttp> source = env.fromElements(NpvHttp.of("1"));
//
//        // split up the lines in pairs (2-tuples) containing: (word,1)
//        DataSet<Integer> counts = source.map(npv -> npv.srcIp)
//                .reduceGroup((GroupReduceFunction<String, Integer>) (values, out) -> {
//                    HashSet<String> accumulator = new HashSet<>();
//                    for (String i : values) {
//                        accumulator.add(i);
//                    }
//                    out.collect(accumulator.size());
//                }).returns(Types.INT);
//
//        counts.print();


        Stream.source(NpvHttp.of(0,"1", 0), NpvHttp.of(0,"1", 0), NpvHttp.of(0,"2", 0)).map(npv -> npv.srcIp).unique().print();
    }


}