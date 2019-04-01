package flink.npv;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class ReferencePipeline<T> implements Stream<T> {

    DataSet<T> source;

    static class Head<T> extends ReferencePipeline<T> {
        final ExecutionEnvironment environment;
//        DataSet<T> source;

        public Head(T[] values) {
            environment = ExecutionEnvironment.getExecutionEnvironment();
            source = environment.fromElements(values);

        }

    }

    static class MapOp<E_IN, E_OUT> extends ReferencePipeline<E_OUT> {

        MapOp(DataSet<E_IN> upstream, MapFunction<E_IN, E_OUT> mapper) {
            source = upstream.map(mapper);
        }


    }

    static class UniqueOperator extends ReferencePipeline<Integer> {

        <E> UniqueOperator(DataSet<E> upstream) {
            source = upstream.reduceGroup((GroupReduceFunction<E, Integer>) (values, out) -> {
                HashSet<E> accumulator = new HashSet<>();
                for (E i : values) {
                    accumulator.add(i);
                }
                out.collect(accumulator.size());
            }).returns(Types.INT);
        }


    }

    static class FilterOperator<E> extends ReferencePipeline<E> {

        FilterOperator(DataSet<E> upstream, FilterFunction<E> filterOp) {
            source = upstream.filter(filterOp);
        }


    }

    @Override
    public final <R> Stream<R> map(MapFunction<T, R> mapper) {
        return new MapOp<>(source, mapper);
    }

    @Override
    public final Stream<Integer> unique() {
        return new UniqueOperator(source);
    }

    @Override
    public final Stream<T> filter(FilterFunction<T> filter) {
        return new FilterOperator(source, filter);
    }

    @Override
    public final Stream<Integer> count() {
        return null;
    }

    @Override
    public final void print() throws Exception {
        source.print();
    }


}