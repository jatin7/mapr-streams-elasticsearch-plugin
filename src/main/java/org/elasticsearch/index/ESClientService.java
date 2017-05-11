package org.elasticsearch.index;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.exception.IndexerESNotRecoverableException;
import org.elasticsearch.index.exception.IndexerESRecoverableException;

public class ESClientService {
	private static final Logger logger = LogManager.getLogger(ESClientService.class);
	private static final String SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";
	private BulkRequestBuilder bulkRequestBuilder;
	private Set<String> indexNames = new HashSet<>();
	private Client client;
	private long sleepBetweenESReconnectAttempts;

	public ESClientService(Client client) {
		this.client = client;
	}

	private void initBulkRequestBuilder(){
		if (bulkRequestBuilder == null){
			bulkRequestBuilder = client.prepareBulk();
		}
	}

	/**
	 * 
	 * @param inputMessage - message body 	
	 * @param indexName - ES index name to index this event into 
	 * @param indexType - index type of the ES 
	 * @param eventUUID - uuid of the event - if needed for routing or as a UUID to use for ES documents; can be NULL
	 * @param routingValue - value to use for ES index routing - if needed; can be null if routing is not needed 
	 * @throws ExecutionException
	 */
	public void addEventToBulkRequest(String inputMessage, String indexName, String indexType, String eventUUID, String routingValue) throws ExecutionException {
		initBulkRequestBuilder();
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, indexType, eventUUID);
		indexRequestBuilder.setSource(inputMessage, XContentType.JSON);
		if (routingValue != null && routingValue.trim().length()>0) {
			indexRequestBuilder.setRouting(routingValue);
		}
		bulkRequestBuilder.add(indexRequestBuilder);
		indexNames.add(indexName);
	}

	public void postToElasticSearch() throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
		try {
			logger.info("Starting bulk posts to ES");

			postBulkToEs(bulkRequestBuilder);
			logger.info("Bulk post to ES finished Ok for indexes: {}; # of messages: {}",
					indexNames, bulkRequestBuilder.numberOfActions());

		} finally {
			bulkRequestBuilder = null;
			indexNames.clear();
		}
	}

	protected void postBulkToEs(BulkRequestBuilder bulkRequestBuilder)
			throws InterruptedException, IndexerESRecoverableException, IndexerESNotRecoverableException {
		BulkResponse bulkResponse = null;
		BulkItemResponse bulkItemResp = null;
		//Nothing/NoMessages to post to ElasticSearch
		if (bulkRequestBuilder.numberOfActions() <= 0) {
			logger.warn("No messages to post to ElasticSearch - returning");
			return;
		}
		try {
			bulkResponse = bulkRequestBuilder.execute().actionGet();
		} catch (NoNodeAvailableException e) {
			// ES cluster is unreachable or down. Re-try up to the configured number of times
			// if fails even after then - throw an exception out to retry indexing the batch
			logger.error("Error posting messages to ElasticSearch: " +
					"NoNodeAvailableException - ES cluster is unreachable, will try to re-connect after sleeping ... ", e);
			Thread.sleep(10000);
			//throw an Exception to re-process the current batch
			throw new IndexerESRecoverableException("Recovering after an NoNodeAvailableException posting messages to Elastic Search " +
					" - will re-try processing current batch");
		} catch (ElasticsearchException e) {
			logger.error("Failed to post messages to ElasticSearch: " + e.getMessage(), e);
			throw new IndexerESRecoverableException(e);
		} 
		logger.debug("Time to post messages to ElasticSearch: {} ms", bulkResponse.getTookInMillis());
		if (bulkResponse.hasFailures()) {
			logger.error("Bulk Message Post to ElasticSearch has errors: {}",
					bulkResponse.buildFailureMessage());
			int failedCount = 0;
			Iterator<BulkItemResponse> bulkRespItr = bulkResponse.iterator();
			//TODO research if there is a way to get all failed messages without iterating over
			// ALL messages in this bulk post request
			while (bulkRespItr.hasNext()) {
				bulkItemResp = bulkRespItr.next();
				if (bulkItemResp.isFailed()) {
					failedCount++;
					String errorMessage = bulkItemResp.getFailure().getMessage();
					String response = bulkItemResp.getFailure().getStatus().name();
					logger.error("Failed Message #{}, response:{}; errorMessage:{}",
							failedCount, response, errorMessage);

					if (SERVICE_UNAVAILABLE.equals(response)){
						logger.error("ES cluster unavailable, thread is sleeping for {} ms, after this current batch will be reprocessed",
								sleepBetweenESReconnectAttempts);
						Thread.sleep(sleepBetweenESReconnectAttempts);
						throw new IndexerESRecoverableException("Recovering after an SERVICE_UNAVAILABLE response from Elastic Search " +
								" - will re-try processing current batch");
					}


					// TODO: there does not seem to be a way to get the actual failed event
					// until it is possible - do not log anything into the failed events log file
					//FailedEventsLogger.logFailedToPostToESEvent(restResponse, errorMessage);
				}
			}
			logger.error("FAILURES: # of failed to post messages to ElasticSearch: {} ", failedCount);
		} 
	}

	public void close() {
		if (client != null) {
			client.close();
		}
	}
}
