package org.elasticsearch.index;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.exception.IndexerESNotRecoverableException;
import org.elasticsearch.index.exception.IndexerESRecoverableException;
//ES permission you should check before doPrivileged() blocks
import org.elasticsearch.SpecialPermission;

public class ConsumerWorker implements Runnable {

	private static final Logger logger = LogManager.getLogger(ConsumerWorker.class);
	private KafkaConsumer<String, String> consumer;
	private final String streamName;
	private final int consumerId;
	// interval in MS to poll for messages, in case there were no
	// messages during the previous interval
	private long pollIntervalMs;
	private static final String CONSUMER_THREAD_NAME_FORMAT = "mapr-streams-elasticsearch-consumer-thread-";
	private ESClientService clientService;

	public ConsumerWorker(int consumerId, String streamName, Properties kafkaProperties,
			long pollIntervalMs, ESClientService clientService) {

		this.consumerId = consumerId;
		this.streamName = streamName;
		this.pollIntervalMs = pollIntervalMs;
		
		SecurityManager sm = System.getSecurityManager();
		if (sm != null) {
		  // unprivileged code such as scripts do not have SpecialPermission
		  sm.checkPermission(new SpecialPermission());
		}
		
		AccessController.doPrivileged(new PrivilegedAction<Object>() {
			public Object run() {
				consumer = new KafkaConsumer<>(kafkaProperties);
				return null;
			}
		});

		this.clientService = clientService;
		logger.info(
				"Created ConsumerWorker with properties: consumerId={}, consumerInstanceName={}, streamName={}, kafkaProperties={}",
				consumerId, streamName, kafkaProperties);
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().setName(CONSUMER_THREAD_NAME_FORMAT+consumerId);
			logger.info("Starting ConsumerWorker, consumerId={}", consumerId);
			SecurityManager sm = System.getSecurityManager();
			if (sm != null) {
			  // unprivileged code such as scripts do not have SpecialPermission
			  sm.checkPermission(new SpecialPermission());
			}

			AccessController.doPrivileged(new PrivilegedAction<Object>() {
				public Object run() {
					logger.info(System.getProperty("java.library.path"));
					consumer.subscribe(Pattern.compile(streamName+":.+"), new NoOpConsumerRebalanceListener());
					return null;
				}
			});

			while (true) {
				boolean isPollFirstRecord = true;
				int numProcessedMessages = 0;
				int numSkippedIndexingMessages = 0;
				int numMessagesInBatch = 0;
				long offsetOfNextBatch = 0;

				logger.debug("consumerId={}; about to call consumer.poll() ...", consumerId);
				ConsumerRecords<String, String> records = consumer.poll(pollIntervalMs);

				// processing messages and adding them to ES batch
				for (ConsumerRecord<String, String> record : records) {
					numMessagesInBatch++;
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());

					logger.info("consumerId={}; recieved record: {}", consumerId, data);
					if (isPollFirstRecord) {
						isPollFirstRecord = false;
						logger.info("Start offset for partition {} in this poll : {}", record.partition(),
								record.offset());
					}

					try {
						clientService.addEventToBulkRequest(record.value(), record.topic(), "fluentd", null, null);
						numProcessedMessages++;
					} catch (Exception e) {
						numSkippedIndexingMessages++;

						logger.error("ERROR processing message {} - skipping it: {}", record.offset(), record.value(),
								e);
						FailedEventsLogger.logFailedToTransformEvent(record.offset(), e.getMessage(), record.value());
					}

				}

				logger.info(
						"Total # of messages in this batch: {}; "
								+ "# of successfully transformed and added to Index: {}; # of skipped from indexing: {}; offsetOfNextBatch: {}",
								numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);

				// push to ES whole batch
				if (!records.isEmpty()) {				
					postToElasticSearch();
				}
			}
		} catch (WakeupException e) {
			logger.warn("ConsumerWorker [consumerId={}] got WakeupException - exiting ... Exception: {}", consumerId,
					e.getMessage());
			// ignore for shutdown
		} 

		catch (IndexerESNotRecoverableException e){
			logger.error("ConsumerWorker [consumerId={}] got IndexerESNotRecoverableException - exiting ... Exception: {}", consumerId,
					e.getMessage());
		}
		catch (Exception e) {
			// TODO handle all kinds of exceptions here - to stop
			// / re-init the consumer when needed
			logger.error("ConsumerWorker [consumerId={}] got Exception - exiting ... Exception: {}", consumerId,
					e.getMessage());
		} finally {
			logger.warn("ConsumerWorker [consumerId={}] is shutting down ...", consumerId);
			consumer.close();
		}
	}

	private boolean postToElasticSearch() throws InterruptedException, IndexerESNotRecoverableException{
		boolean moveToTheNextBatch = true;
		try {
			clientService.postToElasticSearch();
		} catch (IndexerESRecoverableException e) {
			moveToTheNextBatch = false;
			logger.error("Error posting messages to Elastic Search - will re-try processing the batch; error: {}",
					e.getMessage());
		} 

		return moveToTheNextBatch;
	}

	public void shutdown() {
		logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
		consumer.wakeup();
	}

	public int getConsumerId() {
		return consumerId;
	}
}
