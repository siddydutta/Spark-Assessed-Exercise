package bigdata.transformations.filters;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;

import java.time.Instant;
import java.util.Set;

/**
 * Filter function to keep StockPrice{@link StockPrice} instances that meet the following two conditions:
 * 1. StockPrice ticker belongs to the set of asset symbols set via a broadcast variable.
 * 2. StockPrice timestamp is within the (startDate, endDate) time range.
 *
 * @author siddharthad
 */
public class StockPriceFilter implements FilterFunction<StockPrice> {

    private final Broadcast<Set<String>> stockSymbols;
    private final Instant startDate;
    private final Instant endDate;

    /**
     * Constructs a filter for {@link StockPrice} objects.
     *
     * @param stockSymbols A broadcasted set of stock symbols to retain.
     * @param startDate    The start date of the time range.
     * @param endDate      The end date of the time range.
     */
    public StockPriceFilter(Broadcast<Set<String>> stockSymbols, Instant startDate, Instant endDate) {
        this.stockSymbols = stockSymbols;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /**
     * Evaluates whether a {@link StockPrice} instance should be filtered out.
     *
     * @param stockPrice The stock price record to evaluate.
     * @return {@code true} if the stock price passes the filter (i.e., the stock ticker exists
     * in the asset symbol set and its timestamp is within the extended time range),
     * {@code false} otherwise.
     */
    @Override
    public boolean call(StockPrice stockPrice) throws Exception {
        Instant stockTimestamp = TimeUtil.fromDate(stockPrice.getYear(), stockPrice.getMonth(), stockPrice.getDay());
        return this.stockSymbols.value().contains(stockPrice.getStockTicker()) &&
                stockTimestamp.isAfter(this.startDate) && stockTimestamp.isBefore(this.endDate);
    }
}
