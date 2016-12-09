package org.ssd.loganalyzer.model;

import java.io.Serializable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LogData implements Serializable{

	private static final long serialVersionUID = -4512616241467928811L;

	private String username;
	
	private String operationName;
	
	private String logData;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getOperationName() {
		return operationName;
	}

	public void setOperationName(String operationName) {
		this.operationName = operationName;
	}

	public String getLogData() {
		return logData;
	}

	public void setLogData(String logData) {
		this.logData = logData;
	}
	
	public String toJsonString() throws JsonProcessingException{
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(this);
	}
	
}
