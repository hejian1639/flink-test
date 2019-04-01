package flink.npv;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.Serializable;
import java.util.HashSet;

class MapReducer implements Serializable {

    Metrics map(NpvHttp t) {
        Metrics m = new Metrics();
        m.good = t.rspDelayTime < 2000 ? 1 : 0;
        m.total = 1;

        return m;
    }

    Metrics reduce(Metrics a, Metrics b) {
        a.good += b.good;
        a.total += b.total;
        return a;
    }

}