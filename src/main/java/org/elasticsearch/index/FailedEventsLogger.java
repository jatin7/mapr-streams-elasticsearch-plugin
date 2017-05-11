package org.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FailedEventsLogger {

	private static final Logger logger = LogManager.getLogger(FailedEventsLogger.class);

	public static void logFailedEvent(String errorMsg, String event){
		logger.error("General Error Processing Event: ERROR: {}, EVENT: {}", errorMsg, event);
	}

	public static void logFailedToPostToESEvent(String response, String errorMsg){
		logger.error("Error posting event to ES: response: {}, ERROR: {}", response, errorMsg);
	}

	public static void logFailedToTransformEvent(long offset, String errorMsg, String event){
		logger.error("Error transforming event: OFFSET: {}, ERROR: {}, EVENT: {}", 
				offset, errorMsg, event);
	}
	public static void logFailedEvent(long startOffset,long endOffset, int partition ,String errorMsg, String event){
		logger.error("Error transforming event: OFFSET: {} --> {} PARTITION: {},EVENT: {},ERROR: {} ",
				startOffset,endOffset, partition,event,errorMsg);
	}

}
