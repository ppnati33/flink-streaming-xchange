package org.example;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class OrderBookUpdateProcessingTimeAggregate implements AggregateFunction<Instant, Tuple2<Long, Long>, Long> {

    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(Instant eventTimestamp, Tuple2<Long, Long> accumulator) {
        long processingTime = ChronoUnit.MILLIS.between(eventTimestamp, Instant.now());
        return new Tuple2<>(accumulator.f0 + processingTime, accumulator.f1 + 1L);
    }

    @Override
    public Long getResult(Tuple2<Long, Long> accumulator) {
        return (long)(((double) accumulator.f0) / accumulator.f1);
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
