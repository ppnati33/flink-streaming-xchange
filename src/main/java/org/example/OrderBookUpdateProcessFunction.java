package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.binance.BinanceExchange;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.service.marketdata.MarketDataService;

import java.time.Instant;

public class OrderBookUpdateProcessFunction
    extends KeyedProcessFunction<String, CustomOrderBookUpdate, CustomOrderBookUpdate>
    implements CheckpointedFunction {

    private static final Logger logger = Logger.getLogger(OrderBookUpdateProcessFunction.class.getName());

    private transient ValueState<CustomOrderBook> orderBookState;
    private CustomOrderBook orderBook;

    final OutputTag<Instant> outputTag = new OutputTag<Instant>("side-output") {
    };

    @Override
    public void processElement(CustomOrderBookUpdate orderBookUpdate,
                               Context ctx,
                               Collector<CustomOrderBookUpdate> collector) throws Exception {
        try {
            //logger.info("Processing order book update: " + orderBookUpdate.toString());

            // retrieve current order book value
            orderBook = orderBookState.value();

            // initialize the state for the first time
            if (orderBook == null) {
                orderBook = initializeOrderBook();
            }

            orderBook.update(orderBookUpdate);

            /*logger.info(
                "Current order book params: asks size = " + orderBook.getAsks().size() +
                    ", bids size" + orderBook.getBids().size()
            );*/

            // write state back
            orderBookState.update(orderBook);

            // emit event data to regular output
            collector.collect(orderBookUpdate);
            // emit timestamp data to side output
            ctx.output(outputTag, orderBookUpdate.getTimestamp());
        } catch (Exception ex) {
            String errorMessage =
                "Unable to process OrderBookUpdate event: " + orderBookUpdate.toString() +
                    ". Error: " + ex.getMessage();
            logger.error(errorMessage, ex);
            throw ex;
        }
    }

    private CustomOrderBook initializeOrderBook() throws Exception {
        try {
            logger.info("Try to initialize order book");

            Exchange exchange = ExchangeFactory.INSTANCE.createExchange(BinanceExchange.class);
            MarketDataService marketDataService = exchange.getMarketDataService();
            OrderBook orderBook = marketDataService.getOrderBook(CurrencyPair.BTC_USDT, 5000);

            //logger.info("Order book state successfully initialized with a value: " + orderBook.toString());
            logger.info("Order book successfully initialized");

            return CustomOrderBook.from(orderBook);
        } catch (Exception ex) {
            String errorMessage = "Unable to initialize order book because of error: " + ex.getMessage();
            logger.error(errorMessage, ex);
            throw ex;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        orderBookState.clear();
        orderBookState.update(orderBook);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        orderBookState = ctx.getKeyedStateStore().getState(
            new ValueStateDescriptor<>(
                "orderBook",
                TypeInformation.of(new TypeHint<CustomOrderBook>() {
                })
            )
        );
    }
}
