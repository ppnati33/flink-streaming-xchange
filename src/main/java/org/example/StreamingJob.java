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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;
import org.knowm.xchange.dto.marketdata.Trade;

import java.math.BigDecimal;

public class StreamingJob {

    private static final Logger logger = Logger.getLogger(StreamingJob.class.getName());

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStream<Trade> socketInputStream = env.addSource(new CustomSocketStreamFunction());

        socketInputStream
            .filter((FilterFunction<Trade>) trade ->
                trade.getPrice() != null && trade.getPrice().compareTo(BigDecimal.ZERO) > 0
            )
            .print();

        env.execute("Flink Streaming Java API App");
    }
}
