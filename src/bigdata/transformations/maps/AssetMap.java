package bigdata.transformations.maps;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

/**
 * Maps a joined tuple of {@link AssetMetadata} and a broadcasted {@link AssetFeatures} map to an {@link Asset}
 * instance.
 *
 * @author siddharthad
 */
public class AssetMap implements Function<Tuple2<String, AssetMetadata>, Asset> {

    private final Broadcast<Map<String, AssetFeatures>> broadcastAssetFeaturesMap;

    /**
     * Constructs an {@link AssetMap} instance with a broadcasted map of asset features.
     *
     * @param broadcastAssetFeaturesMap A broadcast variable containing a mapping of stock symbols
     *                                  to their computed {@link AssetFeatures}.
     */
    public AssetMap(Broadcast<Map<String, AssetFeatures>> broadcastAssetFeaturesMap) {
        this.broadcastAssetFeaturesMap = broadcastAssetFeaturesMap;
    }

    /**
     * Constructs an {@link Asset} instance by merging metadata with precomputed asset features.
     * If an entry for the stock symbol exists in the broadcasted asset features map, the corresponding
     * {@link AssetFeatures} instance is used. The price-to-earnings (P/E) ratio is also set from the metadata.
     *
     * @param assetMetadataMap A tuple containing the stock symbol and its corresponding {@link AssetMetadata}.
     * @return An {@link Asset} instance populated with metadata and asset features.
     */
    @Override
    public Asset call(Tuple2<String, AssetMetadata> assetMetadataMap) throws Exception {
        AssetFeatures assetFeatures = broadcastAssetFeaturesMap.value().getOrDefault(assetMetadataMap._1(), null);
        AssetMetadata assetMetadata = assetMetadataMap._2();
        if (assetFeatures != null) {
            assetFeatures.setPeRatio(assetMetadata.getPriceEarningRatio());
        }
        return new Asset(assetMetadata.getSymbol(), assetFeatures, assetMetadata.getName(), assetMetadata.getIndustry(), assetMetadata.getSector());
    }
}
