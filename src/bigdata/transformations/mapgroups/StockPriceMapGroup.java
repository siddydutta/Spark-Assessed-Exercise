package bigdata.transformations.mapgroups;

import bigdata.comparators.StockPriceComparator;
import bigdata.objects.AssetFeatures;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import org.apache.spark.api.java.function.MapGroupsFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Computes {@link AssetFeatures} for a given stock by processing its historical {@link StockPrice} instances.
 * For each stock ticker, this class maps the stock volatility using {@link Volitility#calculate(List)}
 * and stock returns using {@link Returns#calculate(int, List)}.
 *
 * @author siddharthad
 */
public class StockPriceMapGroup implements MapGroupsFunction<String, StockPrice, Tuple2<String, AssetFeatures>> {

    private final int numberOfDaysForReturns;

    /**
     * Constructs a {@link StockPriceMapGroup} instance with a specified window for return calculation.
     *
     * @param numberOfDaysForReturns The number of days to use when calculating returns.
     */
    public StockPriceMapGroup(int numberOfDaysForReturns) {
        this.numberOfDaysForReturns = numberOfDaysForReturns;
    }

    /**
     * Computes volatility and returns for a stock based on its historical prices.
     *
     * @param stockSymbol The stock ticker symbol.
     * @param iterator    An iterator over the stock's historical {@link StockPrice} records.
     * @return A tuple containing the stock symbol and its computed {@link AssetFeatures}.
     */
    @Override
    public Tuple2<String, AssetFeatures> call(String stockSymbol, Iterator<StockPrice> iterator) throws Exception {
        List<StockPrice> stockPrices = new ArrayList<>();
        while (iterator.hasNext()) {
            StockPrice stockPrice = iterator.next();
            stockPrices.add(stockPrice);
        }
        // Sort stock prices by date in ascending order
        stockPrices.sort(new StockPriceComparator());
        // Extract closing prices
        List<Double> closePrices = stockPrices.stream().map(StockPrice::getClosePrice).toList();
        // Compute asset features
        AssetFeatures assetFeatures = new AssetFeatures();
        assetFeatures.setAssetVolitility(Volitility.calculate(closePrices));
        assetFeatures.setAssetReturn(Returns.calculate(this.numberOfDaysForReturns, closePrices));
        return Tuple2.apply(stockSymbol, assetFeatures);
    }
}
