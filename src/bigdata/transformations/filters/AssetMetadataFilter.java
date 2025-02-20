package bigdata.transformations.filters;

import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Filters {@link AssetMetadata} records based on the price-to-earnings (P/E) ratio threshold.
 * Filters assets where the P/E ratio is above the specified threshold.
 *
 * @author siddharthad
 */
public class AssetMetadataFilter implements Function<Tuple2<String, AssetMetadata>, Boolean> {

    private final double priceEarningRatioThreshold;

    /**
     * Constructs a filter for {@link AssetMetadata} records.
     *
     * @param priceEarningRatioThreshold The maximum allowable P/E ratio threshold for an asset to be filtered.
     */
    public AssetMetadataFilter(double priceEarningRatioThreshold) {
        this.priceEarningRatioThreshold = priceEarningRatioThreshold;
    }

    /**
     * Evaluates whether an {@link AssetMetadata} record should be filtered out.
     *
     * @param assetAssetMetadata A tuple containing the asset symbol and its metadata.
     * @return {@code true} if the asset's P/E ratio is nonzero and below the threshold, {@code false} otherwise.
     */
    @Override
    public Boolean call(Tuple2<String, AssetMetadata> assetAssetMetadata) throws Exception {
        double assetPriceEarningRatio = assetAssetMetadata._2().getPriceEarningRatio();
        return assetPriceEarningRatio != 0.0d && assetPriceEarningRatio < this.priceEarningRatioThreshold;
    }
}
