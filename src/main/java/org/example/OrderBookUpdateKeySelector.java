package org.example;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.log4j.Logger;

public class OrderBookUpdateKeySelector implements KeySelector<CustomOrderBookUpdate, String> {

    private static final Logger logger = Logger.getLogger(StreamingJob.class.getName());

    public OrderBookUpdateKeySelector() {
    }

    @Override
    public String getKey(CustomOrderBookUpdate orderBookUpdate) throws Exception {
        try {
            return orderBookUpdate.getOrderBookUpdate().getLimitOrder().getInstrument().toString();
        } catch (Exception ex) {
            String errorMessage =
                "Unable to get key for OrderBookUpdate event: " + orderBookUpdate.toString() +
                    ". Error: " + ex.getMessage();
            logger.error(errorMessage, ex);
            throw ex;
        }
    }
}
