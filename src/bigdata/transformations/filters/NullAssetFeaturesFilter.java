package bigdata.transformations.filters;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import org.apache.spark.api.java.function.Function;

/**
 * Filters out {@link Asset} instances that have null {@link AssetFeatures}.
 *
 * @author siddharthad
 */
public class NullAssetFeaturesFilter implements Function<Asset, Boolean> {

    /**
     * Evaluates whether an {@link Asset} instance should be retained.
     *
     * @param asset The asset to filter.
     * @return {@code true} if the asset has non-null {@link AssetFeatures}, {@code false} otherwise.
     */
    @Override
    public Boolean call(Asset asset) throws Exception {
        return asset.getFeatures() != null;
    }
}
