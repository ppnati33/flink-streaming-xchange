package org.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.log4j.Logger;

public class LoggingSink<T> implements SinkFunction<T> {

    private static final Logger logger = Logger.getLogger(LoggingSink.class.getName());

    @Override
    public void invoke(T data, Context context) {
        logger.info(data);
    }
}
