package bigdata.transformations.filters;

import bigdata.objects.AssetFeatures;
import org.apache.spark.api.java.function.FilterFunction;
import scala.Tuple2;

/**
 * Filters {@link AssetFeatures} records based on the volatility ceiling.
 * Filters assets where the volatility is equal to or above the specified threshold.
 *
 * @author siddharthad
 */
public class AssetFeaturesFilter implements FilterFunction<Tuple2<String, AssetFeatures>> {

    private final double volatilityCeiling;

    /**
     * Constructs a filter for {@link AssetFeatures} records.
     *
     * @param volatilityCeiling The maximum allowable volatility for an asset to be filtered.
     */
    public AssetFeaturesFilter(double volatilityCeiling) {
        this.volatilityCeiling = volatilityCeiling;
    }

    /**
     * Evaluates whether an {@link AssetFeatures} record should be filtered out.
     *
     * @param stockAssetFeatures A tuple containing the asset symbol and its computed features.
     * @return {@code true} if the asset's volatility is non-zero and below the threshold, {@code false} otherwise.
     */
    @Override
    public boolean call(Tuple2<String, AssetFeatures> stockAssetFeatures) throws Exception {
        double volatility = stockAssetFeatures._2().getAssetVolitility();
        return volatility != 0.0d && volatility < this.volatilityCeiling;
    }
}
