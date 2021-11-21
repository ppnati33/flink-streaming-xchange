package org.example;

import info.bitrich.xchangestream.binance.BinanceStreamingExchange;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;
import org.knowm.xchange.currency.CurrencyPair;

public class OrderBookUpdateSource extends RichSourceFunction<CustomOrderBookUpdate> {

    private static final Logger logger = Logger.getLogger(OrderBookUpdateSource.class.getName());

    private volatile boolean isRunning;
    private transient BinanceStreamingExchange exchange;
    private transient ProductSubscription subscription;
    private transient Disposable orderBookUpdateData;

    public OrderBookUpdateSource() {
        isRunning = true;
    }

    @Override
    public void run(SourceContext<CustomOrderBookUpdate> ctx) throws Exception {
        logger.info("Initialize order book update subscription");

        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            connect();
            synchronized (lock) {
                orderBookUpdateData = exchange.getStreamingMarketDataService()
                    .getOrderBookUpdates(CurrencyPair.BTC_USDT)
                    .subscribe(
                        orderBookUpdate -> {
                            //logger.info("Received order new book update: " + orderBookUpdate.toString());
                            ctx.collect(new CustomOrderBookUpdate(orderBookUpdate));
                        },
                        throwable -> logger.error("Error in order book update subscription", throwable));
            }
        }
    }

    @Override
    public void cancel() {
        logger.info("Cancel function called");
        isRunning = false;
        orderBookUpdateData.dispose();
        exchange.disconnect().blockingAwait();
    }

    private void connect() {
        subscription = ProductSubscription.create().addOrderbook(CurrencyPair.BTC_USDT).build();
        exchange = create(BinanceStreamingExchange.class);
        exchange.connect(subscription).blockingAwait();
    }

    private <T extends StreamingExchange> T create(Class<T> t) {
        return (T) StreamingExchangeFactory.INSTANCE.createExchange(t);
    }
}
