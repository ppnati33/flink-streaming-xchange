package org.example;

import info.bitrich.xchangestream.bitstamp.v2.BitstampStreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.Trade;

public class CustomSocketStreamFunction extends RichSourceFunction<Trade> {

    private static final Logger logger = Logger.getLogger(CustomSocketStreamFunction.class.getName());

    private volatile boolean isRunning;
    private transient StreamingExchange exchange;
    private transient Disposable subscription;

    public CustomSocketStreamFunction() {
        isRunning = true;
    }

    @Override
    public void run(SourceContext<Trade> sourceContext) throws Exception {
        logger.info("open function called");

        while (isRunning) {
            exchange =
                StreamingExchangeFactory.INSTANCE.createExchange(BitstampStreamingExchange.class);

            exchange.connect().blockingAwait();

            subscription = exchange.getStreamingMarketDataService()
                .getTrades(CurrencyPair.BTC_USD)
                .subscribe(
                    trade -> {
                        logger.info(trade.toString());
                        sourceContext.collect(trade);
                    },
                    throwable -> logger.error("Error in trade subscription", throwable));
        }
    }

    @Override
    public void cancel() {
        logger.info("cancel function called");
        isRunning = false;
        subscription.dispose();
        exchange.disconnect().blockingAwait();
    }
}
