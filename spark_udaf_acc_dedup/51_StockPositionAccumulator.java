//https://raw.githubusercontent.com/mistzzt/ink/16a517c037702f612b90197e882fbe42e4f335e3/benchmarks/flink-udfs/github/StockPositionAccumulator.java
// https://github.com/backstreetbrogrammer/11_JavaSpark/blob/09058231594eec58adc24b0198c6bc097d998368/src/main/java/com/backstreetbrogrammer/chapter16_closures/StockPositionAccumulator.java
package com.backstreetbrogrammer.chapter16_closures;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.util.AccumulatorV2;

import java.util.stream.Collectors;

public class StockPositionAccumulator extends AccumulatorV2<StockPosition, StockPosition> {

    private long count = 0L;
    private StockPosition stockPositionAccumulator;

    public StockPositionAccumulator(final String securityId) {
        stockPositionAccumulator = new StockPosition(securityId);
    }

    @Override
    public boolean isZero() {
        return count == 0L;
    }

    @Override
    public AccumulatorV2<StockPosition, StockPosition> copy() {
        final var securityId = stockPositionAccumulator.getSecurityId();
        final var positionAccumulator = new StockPositionAccumulator(securityId);
        positionAccumulator.count = this.count;
        positionAccumulator.stockPositionAccumulator = this.stockPositionAccumulator;
        return positionAccumulator;
    }

    @Override
    public void reset() {
        count = 0L;
        final var securityId = stockPositionAccumulator.getSecurityId();
        stockPositionAccumulator = new StockPosition(securityId);
    }

    @Override
    public void add(final StockPosition stockPosition) {
        if (stockPosition != null) {
            count += 1;
            final var trades = stockPosition.getTrades();
            if (CollectionUtils.isNotEmpty(trades)) {
                final var securityId = stockPositionAccumulator.getSecurityId();
                final var filteredTrades = trades.stream()
                                                 .filter(trade -> trade.getSecurityId().equals(securityId))
                                                 .collect(Collectors.toList());
                stockPositionAccumulator.addTrades(filteredTrades);
                filteredTrades.forEach(trade -> {
                    final var qty = trade.getExecutedQuantity();
                    final var px = trade.getExecutedPrice();
                    final var notional = qty * px;
                    final var position = stockPositionAccumulator.getPosition();
                    final var profit = stockPositionAccumulator.getProfit();
                    if (trade.getSide() == Side.BUY) {
                        stockPositionAccumulator.setPosition(position + qty);
                        stockPositionAccumulator.setProfit(profit - notional);
                    } else {
                        stockPositionAccumulator.setPosition(position - qty);
                        stockPositionAccumulator.setProfit(profit + notional);
                    }
                });
            }
        }
    }

    @Override
    public void merge(final AccumulatorV2<StockPosition, StockPosition> other) {
        if (other instanceof StockPositionAccumulator) {
            count += ((StockPositionAccumulator) other).count;
            final var otherStockPosition = ((StockPositionAccumulator) other).stockPositionAccumulator;
            final var securityId = stockPositionAccumulator.getSecurityId();
            final var otherSecurityId = otherStockPosition.getSecurityId();
            if (securityId.equals(otherSecurityId)) {
                final var totalPosition = stockPositionAccumulator.getPosition() + otherStockPosition.getPosition();
                stockPositionAccumulator.setPosition(totalPosition);
                final var totalProfit = stockPositionAccumulator.getProfit() + otherStockPosition.getProfit();
                stockPositionAccumulator.setProfit(totalProfit);
                stockPositionAccumulator.addTrades(otherStockPosition.getTrades());
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format("Cannot merge %s with %s%n", this.getClass().getName(), other.getClass().getName()));
        }
    }

    @Override
    public StockPosition value() {
        return stockPositionAccumulator;
    }

    public long count() {
        return count;
    }
}
