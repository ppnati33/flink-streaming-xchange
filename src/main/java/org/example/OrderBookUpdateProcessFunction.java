package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.log4j.Logger;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.binance.BinanceExchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.OrderBookUpdate;
import org.knowm.xchange.service.marketdata.MarketDataService;

import java.time.Instant;

public class OrderBookUpdateProcessFunction
    extends KeyedProcessFunction<String, OrderBookUpdate, Instant> {

    private static final Logger logger = Logger.getLogger(StreamingJob.class.getName());

    private transient ValueState<CustomOrderBook> orderBookState;

    @Override
    public void processElement(OrderBookUpdate orderBookUpdate,
                               Context context,
                               Collector<Instant> collector) throws Exception {
        try {
            logger.info("Processing order book update: " + orderBookUpdate.toString());

            // retrieve the current order book (state)
            orderBookState =
                getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                        "orderBook",
                        TypeInformation.of(new TypeHint<CustomOrderBook>() {
                        })
                    )
                );

            // initialize the state for the first time
            if (orderBookState.value() == null) {
                initializeState();
            }

            CustomOrderBook orderBook =
                Preconditions.checkNotNull(
                    orderBookState.value(),
                    "Order book value is not specified for current state"
                );

            orderBook.update(orderBookUpdate);

            logger.info("Current order book value: " + orderBook.toString());

            // write state back
            orderBookState.update(orderBook);

            collector.collect(Instant.now());
        } catch (Exception ex) {
            String errorMessage =
                "Unable to process OrderBookUpdate event: " + orderBookUpdate.toString() +
                    ". Error: " + ex.getMessage();
            logger.error(errorMessage, ex);
            throw ex;
        }
    }

    private void initializeState() throws Exception {
        try {
            logger.info("Try to initialize state with current order book value");

            Exchange exchange = ExchangeFactory.INSTANCE.createExchange(BinanceExchange.class);
            MarketDataService marketDataService = exchange.getMarketDataService();
            OrderBook orderBook = marketDataService.getOrderBook(CurrencyPair.BTC_USDT);

            logger.info("Order book state successfully initialized with a value: " + orderBook.toString());

            orderBookState.update(CustomOrderBook.from(orderBook));
        } catch (Exception ex) {
            String errorMessage = "Unable to initialize state because of error: " + ex.getMessage();
            logger.error(errorMessage, ex);
            throw ex;
        }
    }
}
