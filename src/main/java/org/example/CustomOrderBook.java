package org.example;

import org.apache.log4j.Logger;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

public class CustomOrderBook implements Serializable {

    private static final Logger logger = Logger.getLogger(CustomOrderBook.class.getName());

    private static final long serialVersionUID = -7788306758114464313L;

    private List<LimitOrder> asks;

    private List<LimitOrder> bids;

    private Date timeStamp;

    public CustomOrderBook() {
    }

    public CustomOrderBook(Date timeStamp, List<LimitOrder> asks, List<LimitOrder> bids) {
        this(timeStamp, asks, bids, false);
    }

    public CustomOrderBook(Date timeStamp, List<LimitOrder> asks, List<LimitOrder> bids, boolean sort) {
        this.timeStamp = timeStamp;
        if (sort) {
            this.asks = new ArrayList(asks);
            this.bids = new ArrayList(bids);
            Collections.sort(this.asks);
            Collections.sort(this.bids);
        } else {
            this.asks = asks;
            this.bids = bids;
        }

    }

    public List<LimitOrder> getAsks() {
        return asks;
    }

    public void setAsks(List<LimitOrder> asks) {
        this.asks = asks;
    }

    public List<LimitOrder> getBids() {
        return bids;
    }

    public void setBids(List<LimitOrder> bids) {
        this.bids = bids;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    public List<LimitOrder> getOrders(Order.OrderType type) {
        return type == Order.OrderType.ASK ? this.asks : this.bids;
    }

    public void update(CustomOrderBookUpdate orderBookUpdate) {
        LimitOrder limitOrder = orderBookUpdate.getOrderBookUpdate().getLimitOrder();
        List<LimitOrder> limitOrders = this.getOrders(limitOrder.getType());
        int idx = Collections.binarySearch(limitOrders, limitOrder);
        if (idx >= 0) {
            limitOrders.remove(idx);
        } else {
            idx = -idx - 1;
        }

        if (orderBookUpdate.getOrderBookUpdate().getTotalVolume().compareTo(BigDecimal.ZERO) != 0) {
            LimitOrder updatedOrder = withAmount(limitOrder, orderBookUpdate.getOrderBookUpdate().getTotalVolume());
            limitOrders.add(idx, updatedOrder);
        }

        this.updateDate(limitOrder.getTimestamp());
    }

    public static CustomOrderBook from(OrderBook orderBook) {
        return new CustomOrderBook(
            orderBook.getTimeStamp(),
            orderBook.getAsks(),
            orderBook.getBids(),
            true
        );
    }

    private static LimitOrder withAmount(LimitOrder limitOrder, BigDecimal tradeableAmount) {
        Order.OrderType type = limitOrder.getType();
        CurrencyPair currencyPair = limitOrder.getCurrencyPair();
        String id = limitOrder.getId();
        Date date = limitOrder.getTimestamp();
        BigDecimal limit = limitOrder.getLimitPrice();
        return new LimitOrder(type, tradeableAmount, currencyPair, id, date, limit);
    }

    private void updateDate(Date updateDate) {
        if (updateDate != null && (this.timeStamp == null || updateDate.after(this.timeStamp))) {
            this.timeStamp = updateDate;
        }

    }

    @Override
    public String toString() {
        return "CustomOrderBook [" +
            "timeStamp=" + timeStamp +
            ", asks=" + asks +
            ", bids=" + bids +
            ']';
    }
}
