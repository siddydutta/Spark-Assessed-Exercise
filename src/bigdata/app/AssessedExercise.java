package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import bigdata.objects.*;
import bigdata.transformations.filters.*;
import bigdata.transformations.grouping.StockPricesGroup;
import bigdata.transformations.mapgroups.StockPriceMapGroup;
import bigdata.transformations.maps.AssetMap;
import bigdata.util.TimeUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;

public class AssessedExercise {

    public static void main(String[] args) throws InterruptedException {

        //--------------------------------------------------------
        // Static Configuration
        //--------------------------------------------------------
        String datasetEndDate = "2020-04-01";
        double volatilityCeiling = 4;
        double peRatioThreshold = 25;

        long startTime = System.currentTimeMillis();

        // The code submitted for the assessed exerise may be run in either local or remote modes
        // Configuration of this will be performed based on an environment variable
        String sparkMasterDef = System.getenv("SPARK_MASTER");
        if (sparkMasterDef == null) {
            File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
            System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
            sparkMasterDef = "local[4]"; // default is local mode with two executors
        }

        String sparkSessionName = "BigDataAE"; // give the session a name

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setMaster(sparkMasterDef)
                .setAppName(sparkSessionName);

        // Create the spark session
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        // Get the location of the asset pricing data
        String pricesFile = System.getenv("BIGDATA_PRICES");
        if (pricesFile == null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries

        // Get the asset metadata
        String assetsFile = System.getenv("BIGDATA_ASSETS");
        if (assetsFile == null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries


        //----------------------------------------
        // Pre-provided code for loading the data
        //----------------------------------------

        // Create Datasets based on the input files

        // Load in the assets, this is a relatively small file
        Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
        //assetRows.printSchema();
        System.err.println(assetRows.first().toString());
        JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());

        // Load in the prices, this is a large file (not so much in data size, but in number of records)
        Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
        Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
        Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects


        AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);

        System.out.println(finalRanking.toString());

        System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");

        Thread.sleep(60000);

        // Close the spark session
        spark.close();

        String out = System.getenv("BIGDATA_RESULTS");
        String resultsDIR = "results/";
        if (out != null) resultsDIR = out;


        long endTime = System.currentTimeMillis();

        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath() + "/SPARK.DONE")));

            Instant sinstant = Instant.ofEpochSecond(startTime / 1000);
            Date sdate = Date.from(sinstant);

            Instant einstant = Instant.ofEpochSecond(endTime / 1000);
            Date edate = Date.from(einstant);

            writer.write("StartTime:" + sdate.toGMTString() + '\n');
            writer.write("EndTime:" + edate.toGMTString() + '\n');
            writer.write("Seconds: " + ((endTime - startTime) / 1000) + '\n');
            writer.write('\n');
            writer.write(finalRanking.toString());
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata, Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {

        // -------------------------------------------------------------------------------------------------------------
        // Investments Ranking Pipeline
        // -------------------------------------------------------------------------------------------------------------

        // -------------------------------------------------------------------------------------------------------------
        // Step 1: Filter assetMetadata Based on the peRatioThreshold
        // Filter operations on assetMetadata is carried out first since the asset metadata file is relatively smaller.
        //
        // The .filter transformation method on a JavaPairRDD is used to apply an implementation of a
        // Function<Tuple2<String, AssetMetadata>, Boolean> to perform this operation. This returns the filtered
        // JavaPairRDD of stock symbols and the associated asset metadata.
        // This transformation reduces 7,458 AssetMetadata objects to 2,817 filteredAssets.
        // -------------------------------------------------------------------------------------------------------------
        JavaPairRDD<String, AssetMetadata> filteredAssets = assetMetadata.filter(new AssetMetadataFilter(peRatioThreshold));

        // The tradeoff to collect the output from this filtering operation may be acceptable as it can help reduce the
        // size of the relatively larger stock price file that need to be processed. Since only the symbols will be
        // required to filter a collect operation is run on the keys of the JavaPairRDD. This involves some data
        // movement to this driver.
        //
        // The set of filtered symbols are converted into a broadcast variable so that a single instance of the set of
        // strings can be shared by multiple tasks in the same node, which is preferable to passing the set in the
        // constructor for each task, which would make several copies of this set.
        // Assuming a Set<String> of 2,817 strings, the size of the broadcast variable ought to be < 50 KiB.
        Broadcast<Set<String>> broadcastFilteredAssetsSymbol = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .broadcast(new HashSet<>(filteredAssets.keys().collect()));

        // -------------------------------------------------------------------------------------------------------------
        // Step 2: Filter StockPrice based on Filtered Symbols and a Date Range
        // The next filter operation on prices is to reduce the count of StockPrice instances to work with in later
        // operations. Since we only required the prices of filtered assets in a one-year range from datasetEndDate,
        // we can filter the remaining prices out.
        // The endDate instance is created based on the provided datasetEndDate, and the startDate instance is created
        // by subtracted 365 days from endDate i.e. from one year before.
        //
        // Finally, the .filter transformation method on a Dataset is used to apply an implementation of a
        // FilterFunction<StockPrice> to perform this operation. While the symbols are passed as a broadcast variable,
        // the two Instant objects can be passed directly via the constructor as the memory footprint is comparatively
        // negligible. This returns the filtered Dataset of StockPrice instances.
        // This transformation reduces 24,196,793 StockPrice objects to 495,715 filteredStockPrices. (~97.85% decrease)
        // -------------------------------------------------------------------------------------------------------------
        Instant endDate = TimeUtil.fromDate(datasetEndDate);
        Instant startDate = endDate.minus(365, ChronoUnit.DAYS);
        Dataset<StockPrice> filteredStockPrices = prices.filter(new StockPriceFilter(broadcastFilteredAssetsSymbol, startDate, endDate));

        // -------------------------------------------------------------------------------------------------------------
        // Step 3: Group Stocks by Symbol
        // Now that we have only the relevant StockPrice records that are required to rank the investments, we have to
        // group them on the basis of which stock they belong to. This is required as technical indicators like
        // volatility and returns are computed on a per-stock basis, using the historical record of the stock's prices.
        //
        // The .groupByKey transformation method is used to apply an implementation of a MapFunction<StockPrice, String>
        // that returns a KeyValueGroupedDataset of the stock symbol and its associated stock prices. Since the map
        // function outputs a string from the StockPrice instance, the Encoders.STRING() is used to build the output.
        // -------------------------------------------------------------------------------------------------------------
        KeyValueGroupedDataset<String, StockPrice> groupedStockPrices = filteredStockPrices.groupByKey(new StockPricesGroup(), Encoders.STRING());

        // -------------------------------------------------------------------------------------------------------------
        // Step 4: Calculate Technical Indicators
        // Now that we have grouped stock prices by their respective symbols, the next step is to compute the technical
        // indicators - volatility and returns for each stock. These indicators are wrapped inside an AssetFeatures
        // instance.
        //
        // The .mapGroups transformation method is applied to process each stock symbol’s grouped price data. This
        // transformation executes an implementation of a
        // MapGroupsFunction<String, StockPrice, Tuple2<String, AssetFeatures>>, where each group is mapped to a Tuple2
        // containing the stock symbol and its computed AssetFeatures.
        // Since the output is a Tuple2 type, the Encoders.tuple function is used to build the output type. The tuple
        // pair is of type Encoders.STRING() for the stock symbol and a Encoders.bean() for the AssetFeatures class.
        //
        // The StockPriceMapGroup class takes an integer parameter in its constructor which represents the number of
        // days to use to compute the returns for the stock. The output is a dataset of 2010 stock symbol, asset
        // features pairs.
        // -------------------------------------------------------------------------------------------------------------
        Dataset<Tuple2<String, AssetFeatures>> assetFeatures = groupedStockPrices.mapGroups(new StockPriceMapGroup(5),
                Encoders.tuple(Encoders.STRING(), Encoders.bean(AssetFeatures.class)));

        // -------------------------------------------------------------------------------------------------------------
        // Step 5: Filter out AssetFeatures Greater than the volatilityCeiling
        // The computed volatility for each stock can help in reducing the acceptable assets by using the volatility
        // ceiling threshold value to filter out more assets.
        //
        // The .filter transformation method is applied to the assetFeatures using an implementation of a
        // FilterFunction<Tuple2<String, AssetFeatures>> which returns a boolean that is used to filter the tuples based
        // on volatility. The output Dataset is converted to a JavaPairRDD using the utility fromJavaRDD function. This
        // does not trigger a Spark job execution as it only involves creating a lower level instance from the Dataset
        // abstraction.
        //
        // This filter reduces 2010 AssetFeatures objects to 371 filteredAssetFeatures.
        // -------------------------------------------------------------------------------------------------------------
        Dataset<Tuple2<String, AssetFeatures>> filteredAssetFeatures = assetFeatures.filter(new AssetFeaturesFilter(volatilityCeiling));
        JavaPairRDD<String, AssetFeatures> filteredAssetFeaturesRDD = JavaPairRDD.fromJavaRDD(filteredAssetFeatures.toJavaRDD());

        // Since there are only 371 filtered asset features, a collect operation is performed here to trigger a Spark
        // job. The collected map of <string, assetfeatures> pairs are then stored in a broadcast variable which is to
        // be used in the next pseudo-join step.
        Broadcast<Map<String, AssetFeatures>> broadcastAssetFeaturesMap = JavaSparkContext.fromSparkContext(spark.sparkContext())
                .broadcast(filteredAssetFeaturesRDD.collectAsMap());

        // -------------------------------------------------------------------------------------------------------------
        // Step 6: Combine AssetFeatures and AssetMetadata based on Stock Symbol to construct Asset
        // At this stage there are two datasets:
        // - broadcastAssetFeatureMap containing a mapping of 371 stock symbol -> asset feature instances.
        // - filteredAssets containing 2,817 <stock symbol, asset metadata> in a JavaPairRDD.
        //
        // The .map transformation on the JavaPairRDD is used to apply an implementation of
        // Function<Tuple2<String, AssetMetadata>, Asset> to merge the metadata with the computed features, which is
        // passed as a broadcast variable. This is preferable to a join as the assetFeaturesMap contains only 371
        // entries keeping the memory overhead of the broadcast variable under 50 KiB avoiding join overheads.
        // This results in a combined list of 2,817 records in the JavaPairRDD<Asset> of assets.
        // The .filter transformation is chained to the resulting JavaRDD to filter out those asset metadata with no
        // asset features i.e. there were no relevant stock prices. The resultant JavaRDD<Asset> has 371 assets.
        // -------------------------------------------------------------------------------------------------------------
        JavaRDD<Asset> assets = filteredAssets.map(new AssetMap(broadcastAssetFeaturesMap)).filter(new NullAssetFeaturesFilter());

        // -------------------------------------------------------------------------------------------------------------
        // Step 7: Sort Assets based on Returns
        // The resulting RDD of Assets can be sorted based on its implementation of the Comparable interface, which
        // sorts Asset based on the value of its returns in the asset features.
        //
        // The .takeOrdered is collect operation which is used extract the top 5 assets. Since assets are ranked
        // based on their value of returns in a decreasing order, the Comparator.reverseOrder() is passed to retrieve
        // the assets with the highest return values.
        //
        // The final ranking is encapsulated in an AssetRanking instance, which holds the top-ranked assets.
        // -------------------------------------------------------------------------------------------------------------
        List<Asset> topAssets = assets.takeOrdered(5, Comparator.reverseOrder());
        AssetRanking finalRanking = new AssetRanking(topAssets.toArray(new Asset[0]));
        return finalRanking;
    }

}
