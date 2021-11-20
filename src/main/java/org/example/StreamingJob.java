package org.example;

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

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

public class StreamingJob {

    private static final String CHECKPOINTS_DIR =
        "file:///usr/local/Cellar/apache-flink/1.14.0/state_backend/rocks_db/checkpoints_dir";
    private static final long CHECKPOINTING_INTERVAL_MS = 20000;
    private static final String JOB_NAME = "Flink Streaming Java API Application";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<CustomOrderBookUpdate> orderBookUpdates = env
            .addSource(new OrderBookUpdateSource())
            .name("order-book-update-event-source");

        final OutputTag<Instant> outputTag = new OutputTag<Instant>("side-output"){};

        DataStream<Instant> timestamps = orderBookUpdates
            .keyBy(new OrderBookUpdateKeySelector())
            .process(new OrderBookUpdateProcessFunction())
            .name("order-book-update-event-processor")
            .getSideOutput(outputTag);

        timestamps
            .addSink(new LoggingSink())
            .name("logging-sink");

        env
            .getCheckpointConfig()
            .setCheckpointStorage(CHECKPOINTS_DIR);

        env
            //.setParallelism(4)
            .enableCheckpointing(CHECKPOINTING_INTERVAL_MS)
            //.setStateBackend(new HashMapStateBackend())
            .setStateBackend(new EmbeddedRocksDBStateBackend())
            .execute(JOB_NAME);
    }
}