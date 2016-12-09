package org.ssd.loganalyzer;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.ssd.loganalyzer.receiver.RedisCustomReceiver;

import scala.Tuple2;

public class StreamingLogAnomalyDetector {

	private static final String APP_NAME = "StreamingLogAnomalyDetector";
	
	private static final String REDIS_HOST = "localhost";
	
	private static final int REDIS_PORT = 32768;
	
	private static final String K_MEANS_MODEL_PATH = "Users/slobodandjurdjevic/Documents/msds_projects/LogSecurity/model/";
	
	public static void main( String[] args ) throws Exception{
		SparkConf conf = new SparkConf()
					.setAppName(APP_NAME);
		
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
		
		final KMeansModel kmeansModel = KMeansModel.load(K_MEANS_MODEL_PATH + "KMeansModel");
		
		JavaDStream<List<Tuple2<String, String>>> customRedisLogDataStream = 
				ssc.receiverStream(new RedisCustomReceiver(StorageLevel.MEMORY_AND_DISK_2(),REDIS_HOST, REDIS_PORT));
		
		JavaDStream<List<Tuple2<String, String>>> logAnomalies = 
				customRedisLogDataStream.filter(new Function<List<Tuple2<String, String>>, Boolean>(){

					public Boolean call(List<Tuple2<String, String>> logStatements) throws Exception {
						for (Tuple2<String,String> logStatement : logStatements){
							System.out.println("Key = " + logStatement._1());
							System.out.println("Value = " + logStatement._2());
							
						}
						return true;
					}
				});
		logAnomalies.foreachRDD(new VoidFunction<JavaRDD<List<Tuple2<String, String>>>>(){

			public void call(JavaRDD<List<Tuple2<String, String>>> anomalies) throws Exception {
				List<List<Tuple2<String, String>>> logsToRecord = anomalies.collect();
				for (List<Tuple2<String, String>> logToRecord : logsToRecord){
					for (Tuple2<String,String> logStatement : logToRecord){
						System.out.println("Key = " + logStatement._1());
						System.out.println("Value = " + logStatement._2());
					}
				}
			}

		});
		
		ssc.start();
	    ssc.awaitTermination();
	}
}
