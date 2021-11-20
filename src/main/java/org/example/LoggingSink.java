package org.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.log4j.Logger;

import java.time.Instant;

public class LoggingSink implements SinkFunction<Instant> {

    private static final Logger logger = Logger.getLogger(LoggingSink.class.getName());

    @Override
    public void invoke(Instant eventTimestamp, Context context) {
        logger.info(eventTimestamp);
    }
}
