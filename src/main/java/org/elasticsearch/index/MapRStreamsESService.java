/**
 * 
 */
package org.elasticsearch.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author ntirupattur
 *
 */
public class MapRStreamsESService extends AbstractLifecycleComponent {

	private final Injector injector;
	private Client client;
	private static final Logger logger = LogManager.getLogger(MapRStreamsESService.class);
	private final static String CONSUMER_PROPERTIES = "consumer.properties";
	public static final Properties properties;
	private String streamName, consumerGroup;
	private int numPartitions,pollIntervalInMs;
	private ExecutorService consumersThreadPool = null;
	private List<ConsumerWorker> consumers = new ArrayList<>();
	private ESClientService clientService;


	@Inject
	public MapRStreamsESService(Settings settings,Injector injector) {
		super(settings);
		this.injector = injector;
		this.streamName = properties.getProperty("streamname", "/var/mapr/mapr.monitoring/logsmonitoring");
		this.consumerGroup = properties.getProperty("groupId", "logsConsumerGroup");
		this.numPartitions = Integer.parseInt(properties.getProperty("partitions", "10"));
		this.pollIntervalInMs = Integer.parseInt(properties.getProperty("pollinterval", "10000"));
	}

	@Override
	protected void doStart() {
		logger.info("Getting the client");
		this.client = injector.getInstance(Client.class);
		init();
	}

	@Override
	protected void doStop() {
		clientService.close();
		shutdownConsumers();
		if(this.client != null) this.client.close();
	}

	@Override
	protected void doClose() throws IOException {
		// TODO Auto-generated method stub

	}

	private void init() {
		Properties props = new Properties();
		props.put("key.deserializer",
				org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put("value.deserializer",
				org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put("group.id", this.consumerGroup);
		props.put("auto.offset.reset", "earliest");
		//TODO - Should we add auto commit config ?
		clientService = new ESClientService(client);
		initConsumers(numPartitions, props);
	}

	private void initConsumers(int consumerPoolCount, Properties props) {
		logger.info("initConsumers() started, consumerPoolCount={}", consumerPoolCount);
		consumers = new ArrayList<>();
		consumersThreadPool = Executors.newFixedThreadPool(consumerPoolCount);
		for (int consumerNumber = 0; consumerNumber < consumerPoolCount; consumerNumber++) {
			ConsumerWorker consumer = new ConsumerWorker(consumerNumber, streamName, props, pollIntervalInMs, clientService);
			consumers.add(consumer);
			consumersThreadPool.submit(consumer);
		}
	}

	private void shutdownConsumers() {
		logger.info("shutdownConsumers() started ....");
		if (consumers != null) {
			for (ConsumerWorker consumer : consumers) {
				consumer.shutdown();
			}
		}
		if (consumersThreadPool != null) {
			consumersThreadPool.shutdown();
			try {
				consumersThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				logger.warn("Got InterruptedException while shutting down consumers, aborting");
			}
		}
		logger.info("shutdownConsumers() finished");
	}

	static {
		properties = new Properties();
		try {
			properties.load(MapRStreamsESService.class.getClassLoader().getResourceAsStream(CONSUMER_PROPERTIES));
		} catch (IOException e) {
			logger.error("Can not find [{}] resource in the class loader", CONSUMER_PROPERTIES);
			throw new RuntimeException(e);
		}
	}

}
