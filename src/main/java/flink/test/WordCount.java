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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.Types;

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
 *
 */
public class WordCount {

    //
    //	Program
    //
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
//    private static final Logger LOG = LogManager.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        LOG.info("WordCount.main");

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        LOG.info("ExecutionEnvironment env={}", env);

        // get input data
        DataSet<String> text = env.fromElements(
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,"
        );

        LOG.info("DataSet<String> text = {}", text);

        DataSet<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    // normalize and split the line
                    String[] tokens = value.toLowerCase().split("\\W+");
                    LOG.info("tokens.length= {}", tokens.length);

                    // emit the pairs
                    for (String token : tokens) {
                        if (token.length() > 0) {
                            out.collect(new Tuple2<>(token, 1));
                        }
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        LOG.info("DataSet<Tuple2<String, Integer>> counts.count= {}", counts.count());


        // execute and print result
//        counts.print();
        counts.output(new Utils.CollectHelper<>(new AbstractID().toString(),  
                counts.getType().createSerializer(env.getConfig()))).name("collect()");
        env.execute();
    }


}
