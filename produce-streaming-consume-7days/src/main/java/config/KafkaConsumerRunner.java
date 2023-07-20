package config;

import java.util.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import SpringWebApplication.WSController;

public class KafkaConsumerRunner implements Runnable {
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private final Properties props;
	private final int pull_waitingTime;
	private final JsonParser Jparser;
	private final AtomicBoolean sentToWebSocket;
	private final Logger log = LoggerFactory.getLogger(KafkaConsumerRunner.class);
	public KafkaConsumerRunner(Properties props, String topic, boolean sentToWebSocket) {
		this.props = props;
		this.pull_waitingTime = (int) props.get("auto.commit.interval.ms");
		this.Jparser = JsonParserFactory.getJsonParser();
		consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
		this.sentToWebSocket = new AtomicBoolean(sentToWebSocket);
	}

	@Override
	public void run() {
		try {
			Map<String, TreeMap<Long,String>> recordKey = new HashMap<String, TreeMap<Long,String>>();
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(pull_waitingTime);
				if (records.count() > 0) {
					for (ConsumerRecord<String, String> record : records) {	
						if (this.sentToWebSocket.get()) {
							String market = record.key();				
							if(!recordKey.containsKey(market)) recordKey.put(market, new TreeMap<Long,String>());
							TreeMap<Long,String> windows = recordKey.get(market);
							int index = record.value().indexOf(',');
							long window_start = Long.valueOf(record.value().substring(index+16, index+29));
							windows.put(window_start,record.value());	
							if(windows.size()>2) {
								SimpMessagingTemplate template = WSController.getTemplate();
								long start = windows.firstKey();
								Map<String, Object> JsonMap = Jparser.parseMap(windows.get(start));
								String currentTput = (String) JsonMap.get("UTput");
								String count = (String) JsonMap.get("count");
								template.convertAndSend("/topic/currentTput", market + "," + currentTput + "," +  start);
								windows.remove(start);
								log.info("start = {}, client = {}, count = {}, balance = {}", start, market, count, currentTput);
							}
						}
					}
					if (this.props.getProperty("enable.auto.commit").equals("false"))
						consumer.commitSync();
				}
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}
}
