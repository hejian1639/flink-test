package flink.npv;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

public interface Stream<T> {


    <R> Stream<R> map(MapFunction<T, R> mapper);

    Stream<Integer> unique();

    Stream<T> filter(FilterFunction<T> filter);

    Stream<Integer> count();

    void print() throws Exception;

    static <T> Stream<T> source(T... values) {
        return new ReferencePipeline.Head<>(values);
    }


}
