package bigdata.comparators;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;

import java.util.Comparator;

/**
 * Comparator for sorting {@link StockPrice} instances by date in ascending order.
 * The comparison is based on the stock's year, month, and day values.
 *
 * @author siddharthad
 */
public class StockPriceComparator implements Comparator<StockPrice> {

    /**
     * Compares two {@link StockPrice} instances based on their timestamps.
     *
     * @param stockPrice1 The first stock price.
     * @param stockPrice2 The second stock price.
     * @return A negative value if {@code stockPrice1} is earlier than {@code stockPrice2}, zero if they are equal,
     * and a positive value if {@code stockPrice1} is later than {@code stockPrice2}.
     */
    @Override
    public int compare(StockPrice stockPrice1, StockPrice stockPrice2) {
        return TimeUtil.fromDate(stockPrice1.getYear(), stockPrice1.getMonth(), stockPrice1.getDay())
                .compareTo(TimeUtil.fromDate(stockPrice2.getYear(), stockPrice2.getMonth(), stockPrice2.getDay()));
    }
}
