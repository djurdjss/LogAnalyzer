package org.ssd.loganalyzer.repository;

import java.io.Serializable;
import java.util.List;

import org.ssd.loganalyzer.model.LogData;

import com.fasterxml.jackson.core.JsonProcessingException;

import scala.Tuple2;

public interface RedisLogRepository extends Serializable{

	public List<Tuple2<String,String>> getDataFromRedis();
	
	public void recordBadLogs(List<LogData> badLogData, String keyPrefix) throws JsonProcessingException;
	
	public void recordProcessedLogs();
}
