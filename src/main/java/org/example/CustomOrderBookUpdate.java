package org.example;

import org.knowm.xchange.dto.marketdata.OrderBookUpdate;

import java.io.Serializable;
import java.time.Instant;

public class CustomOrderBookUpdate implements Serializable {
    private static final long serialVersionUID = -7283757982319511256L;

    private final OrderBookUpdate orderBookUpdate;
    private final Instant timestamp;

    public CustomOrderBookUpdate(OrderBookUpdate orderBookUpdate) {
        this.orderBookUpdate = orderBookUpdate;
        this.timestamp = Instant.now();
    }

    public OrderBookUpdate getOrderBookUpdate() {
        return orderBookUpdate;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "CustomOrderBookUpdate [" +
            "orderBookUpdate=" + orderBookUpdate +
            ", timestamp=" + timestamp +
            ']';
    }
}
