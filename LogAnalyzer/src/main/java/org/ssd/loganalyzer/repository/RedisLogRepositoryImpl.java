package org.ssd.loganalyzer.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.ssd.loganalyzer.model.LogData;

import com.fasterxml.jackson.core.JsonProcessingException;

import redis.clients.jedis.Jedis;
import scala.Tuple2;

public class RedisLogRepositoryImpl implements RedisLogRepository{

	private static final long serialVersionUID = -3250488146726723541L;

	private String host;
	
	private int port;
	
	public RedisLogRepositoryImpl(String host, int port){
		this.host = host;
		this.port = port;
	}
	
	public List<Tuple2<String,String>> getDataFromRedis(){
		Jedis jedis = new Jedis(host,port);
		Set<String> keys = jedis.keys("SIM1*");
		List<Tuple2<String,String>> logsData = new ArrayList<Tuple2<String,String>>();
		for (String key:keys){
			logsData.add(new Tuple2<String, String>(key, jedis.get(key)));
		}
		jedis.close();
		return logsData;
	}
	
	public void recordBadLogs(List<LogData> badLogData, String keyPrefix) throws JsonProcessingException{
		Jedis jedis = new Jedis(host,port);
		for (LogData badLog : badLogData){
			String key = "BAD_" + keyPrefix + UUID.randomUUID().toString();
			jedis.set(key, badLog.toJsonString());
		}
		jedis.close();
	}

	public void recordProcessedLogs() {
		// TODO Auto-generated method stub
		
	}
	
}
