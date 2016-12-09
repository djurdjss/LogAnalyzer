package org.ssd.loganalyzer;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StandardScalerModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.ssd.loganalyzer.model.LogData;
import org.ssd.loganalyzer.repository.RedisLogRepository;
import org.ssd.loganalyzer.repository.RedisLogRepositoryImpl;

import com.fasterxml.jackson.core.JsonProcessingException;

import scala.Tuple2;

public class LogModelGenerator {

	private static final String APP_NAME = "LogAnalyzer";
	
	private static final String REDIS_HOST = "localhost";
	
	private static final int REDIS_PORT = 32768;
	
	private static final String KEY_PREFIX = "SIM1";
	
	private static final String K_MEANS_MODEL_PATH = "/Users/slobodandjurdjevic/Documents/msds_projects/LogSecurity/model/";
	
	public static void main( String[] args ) throws IOException{
		SparkConf conf = new SparkConf()
					.setAppName(APP_NAME);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		final SparkSession sparkSession = SparkSession.builder().appName(APP_NAME).getOrCreate();
		
		JavaRDD<Tuple2<String, String>> dataLogs = getDataFromRedis(sc);
		
		JavaRDD<String> logStatements = dataLogs.map(new Function<Tuple2<String,String>,String>(){

			public String call(Tuple2<String, String> logData) throws Exception {
				return logData._2;
			}
			
		});
		
		Dataset<Row> logMessages = sparkSession.read().json(logStatements);
		
		Dataset<Row> logDataToCluster = 
				logMessages.select(new Column("username"),
								   new Column("operationName"),
								   new Column("logData"));
		
		logDataToCluster.show();
		
		JavaRDD<Row> rowsOfDataToClusterRDD = logDataToCluster.javaRDD();
		
		StructType schema = new StructType(new StructField[]{
				createStructField("username", StringType, false),
			    createStructField("operationName", StringType, false),
			    createStructField("logData", StringType, false)
		});
		
		
		
		Dataset<Row> rowsOfDataToClusterDataSet = 
				sparkSession.createDataFrame(rowsOfDataToClusterRDD, schema);
		
		for (String column : rowsOfDataToClusterDataSet.columns()){
			StringIndexer indexer = new StringIndexer();
			indexer.setInputCol(column);
			indexer.setOutputCol(column + "_index");
			rowsOfDataToClusterDataSet = 
					indexer.fit(rowsOfDataToClusterDataSet).transform(rowsOfDataToClusterDataSet);
		}
		
		rowsOfDataToClusterDataSet.show();
	

		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(new String[]{"username_index","operationName_index","logData_index"});
		vectorAssembler.setOutputCol("features");
		Dataset<Row> readyForScaling = vectorAssembler.transform(rowsOfDataToClusterDataSet);
		
		StandardScaler scaler = new StandardScaler();
		scaler.setInputCol("features");
		scaler.setOutputCol("scaledFeatures");
		scaler.setWithMean(false);
		scaler.setWithStd(true);
		
		StandardScalerModel scalerModel = scaler.fit(readyForScaling);
		
		Dataset<Row> modelReadyDF_Scaled =  scalerModel.transform(readyForScaling);
		modelReadyDF_Scaled.show();
		
		
//		# Now, subset the feature_scaled column and rename it to features #"Primary",
		Dataset<Row> readyForClustering = modelReadyDF_Scaled
				.selectExpr
					("logData","operationName","username","username_index",
							"logData_index","operationName_index","scaledFeatures as features");

		readyForClustering.show();
		
	
		//Create Gaussian mixed model
		//train the model
		GaussianMixture gmm = new GaussianMixture().setK(3);
		GaussianMixtureModel gmmModel = gmm.fit(readyForClustering);
		Dataset<Row> transformedGMM = gmmModel.transform(readyForClustering);
		
		transformedGMM.show();
		transformedGMM.groupBy("prediction").count().show();
		
		// Create kmeans model
		//train the model
		KMeans kmeans = new KMeans().setK(7).setSeed(1);
		KMeansModel kmeansModel = kmeans.fit(readyForClustering);

		Dataset<Row> transformed = kmeansModel.transform(readyForClustering);
		transformed.show();
		
		//group by the clusters
		transformed.groupBy("prediction").count().show();


		//obtain count of the number of items in the dataframe
		long count = transformed.count();

		//group by prediction and count the number in each group
		Dataset<Row> kmeansGroup = transformed.groupBy("prediction").count();
		kmeansGroup.show();

		//filter for clusters that have fewer members than 40% of the population
	
		Dataset<Row> filteredKmeansGroup = kmeansGroup.filter(kmeansGroup.col("count").leq(count * .05));
		filteredKmeansGroup.show();		
		
		
		
		transformed.registerTempTable("kmeans");
		filteredKmeansGroup.registerTempTable("subset");

		Dataset<Row> anomalies = 
				sparkSession
					.sqlContext()
						.sql("SELECT a.logData, a.operationName, a.username, a.prediction AS Cluster " + 
		                           "FROM kmeans as a INNER JOIN subset AS b " +
		                           "ON a.prediction = b.prediction");
		
		anomalies.show();
		
		
		List<Row> anolamiesList = anomalies.collectAsList();
		persistAnomaliesToRedis(anolamiesList, KEY_PREFIX);
		//kmeansModel.save(K_MEANS_MODEL_PATH + "KMeansModel");
		//KMeansModel result = KMeansModel.load(K_MEANS_MODEL_PATH + "KMeansModel");
	
		sc.stop(); 
	}
	
	private static JavaRDD<Tuple2<String, String>> getDataFromRedis(JavaSparkContext sc){
		RedisLogRepository redisLogRepository = new RedisLogRepositoryImpl(REDIS_HOST, REDIS_PORT);
		return sc.parallelize(redisLogRepository.getDataFromRedis());
	}
	
	private static void persistAnomaliesToRedis(List<Row> anomaliesList, String keyPrefix) throws JsonProcessingException{
		RedisLogRepository redisLogRepository = new RedisLogRepositoryImpl(REDIS_HOST, REDIS_PORT);
		List<LogData> badLogData = new ArrayList<LogData>();
		for (Row anomalie : anomaliesList){
			LogData logData = new LogData();
			logData.setLogData(anomalie.getString(0));
			logData.setOperationName(anomalie.getString(1));
			logData.setUsername(anomalie.getString(2));
			badLogData.add(logData);
		}
		redisLogRepository.recordBadLogs(badLogData, keyPrefix);
	}
	
}