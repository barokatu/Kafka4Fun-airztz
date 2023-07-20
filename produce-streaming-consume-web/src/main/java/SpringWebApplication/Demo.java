package SpringWebApplication;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import config.KafkaConfiguration;
import config.KafkaConsumerRunner;
import config.KafkaProducerRunner;
import config.KafkaStreamingLogic;

public class Demo {

	public static void runDemo(String intputTopic, String outputTopic) throws InterruptedException {
		
		// Producer Demo
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConfiguration.BrokerURL);
		props.put("metrics.recording.level", "DEBUG");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// mimic 3 users sending data to kafka topic "streams-file-input":
		KafkaProducerRunner ProducerRunner1 = new KafkaProducerRunner(props, intputTopic, "csv/Colorado_20170708.csv");
		KafkaProducerRunner ProducerRunner2 = new KafkaProducerRunner(props, intputTopic, "csv/Manhattan_20170708.csv");
		KafkaProducerRunner ProducerRunner3 = new KafkaProducerRunner(props, intputTopic, "csv/Oregon_20170708.csv");
		KafkaProducerRunner ProducerRunner4 = new KafkaProducerRunner(props, intputTopic, "csv/Jakarta_20170708.csv");
		KafkaProducerRunner ProducerRunner5 = new KafkaProducerRunner(props, intputTopic, "csv/Surabaya_20170708.csv");
		new Thread(ProducerRunner1).start(); // producer will close once finish reading the input
		new Thread(ProducerRunner2).start(); // producer will close once finish reading the input
		new Thread(ProducerRunner3).start(); // producer will close once finish reading the input
		new Thread(ProducerRunner4).start(); // producer will close once finish reading the input
		new Thread(ProducerRunner5).start(); // producer will close once finish reading the input		
		
		// Stream Analysis DEMO

		// Stream processing is not easy if you choose to

		// (1) DIY:
		// while(consumerisRunning){
		// message = consumer.poll();

		// DIY your analysis here:

		// producer.sent(message);
		// }
		// How do you Ordering the messages if you get them from different
		// topics?
		// How do you Partitioning the messages and Scale out your processing?
		// How do you handle fault tolerance&re-processing the data
		// How do you manage the state of your windowing analysis to achieve
		// exactly-one analysis

		// (2) reply on full-fledged stream processing system:
		// Storm, Spark, Samza
		//

		// (3) Streams API
		// A unique feature of the Kafka Streams API is that the applications
		// you build with it are normal Java applications.
		// These applications can be packaged, deployed, and monitored like any
		// other Java application –
		// there is no need to install separate processing clusters or similar
		// special-purpose and expensive infrastructure!

		props = new Properties();
		props.put("metrics.recording.level", "DEBUG");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-analysis");
		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KafkaStreamingLogic.precessing_interval);// The frequency with which to save the position of the processor.
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "6");
		props.put(StreamsConfig.POLL_MS_CONFIG, "100");// The amount of time in milliseconds to block waiting for input.
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfiguration.BrokerURL);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// use 1 stream client with 6 threads to analysis
		//KStreamBuilder LogicBuilder = KafkaStreamingLogic.TputByMarket_LogicBuilder(intputTopic, outputTopic);
		KStreamBuilder LogicBuilder = KafkaStreamingLogic.TputByMarket_LogicBuilder_Windowing(intputTopic, outputTopic);
		KafkaStreams streams1 = new KafkaStreams(LogicBuilder, props);
		streams1.cleanUp();
		streams1.start();

		// CONSUMER DEMO
		props = new Properties();
		props.put("metrics.recording.level", "DEBUG");
		props.put("bootstrap.servers", KafkaConfiguration.BrokerURL);
		props.put("group.id", "JAVAConsumerGroup1");
		props.put("session.timeout.ms", "100000");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", KafkaStreamingLogic.precessing_interval);// The frequency in
														// milliseconds that the
														// consumer offsets are
														// auto-committed to
														// Kafka if
														// enable.auto.commit is
														// set to true
		// props.put("auto.offset.reset", "earliest");
		props.put("auto.offset.reset", "latest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// use three consumers clients to consume
		boolean sentToWebSocket = true;
		// sentToWebSocket = false;
		KafkaConsumerRunner ConsumerRunner1 = new KafkaConsumerRunner(props, outputTopic, sentToWebSocket);
		KafkaConsumerRunner ConsumerRunner2 = new KafkaConsumerRunner(props, outputTopic, sentToWebSocket);
		KafkaConsumerRunner ConsumerRunner3 = new KafkaConsumerRunner(props, outputTopic, sentToWebSocket);
		new Thread(ConsumerRunner1).start();
		new Thread(ConsumerRunner2).start();
		new Thread(ConsumerRunner3).start();
		
		// waiting for producing completion
		while (ProducerRunner1.getStatus() || ProducerRunner2.getStatus() || ProducerRunner3.getStatus()){
			TimeUnit.MILLISECONDS.sleep(10 * 1000);
			continue;
		}
		System.out.println("Finished!");
		streams1.close();
		streams1.cleanUp();
		ConsumerRunner1.shutdown();
		ConsumerRunner2.shutdown();
		ConsumerRunner3.shutdown();
		Runtime.getRuntime().addShutdownHook(new Thread(streams1::close));
	}

	public static void main(String[] args) {
		String a = "Date:20170704_Region:_Market:";
		String[] k = a.split(":");
		System.out.println(k.length);
	}
}
