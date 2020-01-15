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

import flink.test.wordcount.WordCount;
import flink.test.wordcount.util.WordCountData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Skeleton for a Flink Batch Job.
 * <p>
 * For a full example of a Flink Batch Job, see the WordCountJob.java file in the
 * same package/directory or have a look at the website.
 * <p>
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * target/flink-test-1.0.jar
 * From the CLI you can then run
 * ./bin/flink run -c pom.ConnectJob target/flink-test-1.0.jar
 * <p>
 * For more information on the CLI see:
 * <p>
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
@Slf4j
public class ConnectJob {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            text = env.fromElements(WordCountData.WORDS);
        }


        DataStream<String> filter = env.fromElements(new String[]{"be", "not"});

        // split up the lines in pairs (2-tuples) containing: (word,1)
        text.flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            log.info("tokens.length= {}", tokens.length);

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .connect(filter).process(
                new CoProcessFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
                    List<String> filterWords;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        filterWords = new ArrayList<>();

                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println(" processElement1 thread: " + Thread.currentThread());
                        if (filterWords.contains(value.f0)) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println(" processElement2 thread: " + Thread.currentThread());
                        filterWords.add(value);
                    }

                })                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .sum(1).print();

        // execute program
        System.out.println(env.getExecutionPlan());
        env.execute("Streaming WordCount");
    }
}
