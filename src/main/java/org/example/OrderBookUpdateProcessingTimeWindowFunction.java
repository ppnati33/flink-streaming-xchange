package org.example;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OrderBookUpdateProcessingTimeWindowFunction
    extends ProcessAllWindowFunction<Long, Long, TimeWindow> {

    @Override
    public void process(Context ctx, Iterable<Long> averages, Collector<Long> out) throws Exception {
        Long average = averages.iterator().next();
        out.collect(average);
    }
}
