package org.ssd.loganalyzer.receiver;

import java.util.List;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.ssd.loganalyzer.repository.RedisLogRepositoryImpl;

import scala.Tuple2;

public class RedisCustomReceiver extends Receiver<List<Tuple2<String,String>> >{

	private static final long serialVersionUID = 6989990450336695396L;
	
	private RedisLogRepositoryImpl redisLogRepository;
	
	public RedisCustomReceiver(StorageLevel storageLevel,String redisHost, int redisPort) {
		super(storageLevel);
		redisLogRepository = new RedisLogRepositoryImpl(redisHost, redisPort);
	}

	@Override
	public void onStart() {
		new Thread()  {
		     @Override public void run() {
		       receive();
		     }
		}.start();
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub	
	}

	private void receive(){
		store(redisLogRepository.getDataFromRedis());
	}
}
