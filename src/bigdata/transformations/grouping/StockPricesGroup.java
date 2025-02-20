package bigdata.transformations.grouping;

import bigdata.objects.StockPrice;
import org.apache.spark.api.java.function.MapFunction;

/**
 * Maps a {@link StockPrice} instance to its stock ticker symbol.
 * Used for grouping stock prices by ticker.
 *
 * @author siddharthad
 */
public class StockPricesGroup implements MapFunction<StockPrice, String> {

    /**
     * Extracts the stock ticker from a {@link StockPrice} object.
     *
     * @param stockPrice The stock price record.
     * @return The stock ticker symbol.
     */
    @Override
    public String call(StockPrice stockPrice) throws Exception {
        return stockPrice.getStockTicker();
    }
}
