package flink.test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 */
public class StreamFibonacci {

    //
    //	Program
    //

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IterativeStream<Tuple3<Integer, Integer, Integer>> it =
                env.socketTextStream("localhost", 9999).map(s -> {
                    Integer i = Integer.valueOf(s);
                    return new Tuple3<>(1, 2, i);
                }).returns(Types.TUPLE(Types.INT, Types.INT, Types.INT)).iterate();

        SplitStream<Tuple3<Integer, Integer, Integer>> step =
                it.map(value -> new Tuple3<>(value.f1, value.f0 + value.f1, value.f2 - 1))
                        .returns(Types.TUPLE(Types.INT, Types.INT, Types.INT))
                        .split(value -> {
                            List<String> output = new ArrayList<>();
                            if (value.f2 > 0) {
                                output.add("iterate");

                            } else {
                                output.add("output");

                            }
                            return output;

                        });

        it.closeWith(step.select("iterate"));

        step.select("output").map(value -> value.f1).print();

        env.execute("Fibonacci");

    }


}
