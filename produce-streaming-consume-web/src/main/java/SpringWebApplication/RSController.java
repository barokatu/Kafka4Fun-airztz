package SpringWebApplication;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import config.KafkaConfiguration;;

//this functionality is not used currently
@RestController
@RequestMapping(value = "/")
public class RSController {
	@Autowired
	static SimpMessagingTemplate template;
	private Double currentTputDefault = 0.0;
	private Double currentTput = currentTputDefault;
	private String currentplayUnitDefault = "0";
	private String currentplayUnit = currentplayUnitDefault;
	private String currentbufferedUnitDefault = "0";
	private String currentbufferedUnit = currentbufferedUnitDefault;
	private String currentplayStatusDefault = "NONE";
	private String currentplayStatus = currentplayStatusDefault;
	private int bufferVariationDefault = 0;
	private int bufferVariation = bufferVariationDefault;
	private JsonParser Jparser = JsonParserFactory.getJsonParser();
	private Producer<String, String> producer;

	RSController() {
		Properties props = new Properties();
		props = new Properties();
		props.put("bootstrap.servers", KafkaConfiguration.BrokerURL);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
	}

	// The producer is responsible for choosing which record to assign to which
	// partition within the topic.
	// This can be done in a round-robin fashion simply to balance load or it
	// can be done according to some semantic partition function (say based on
	// some key in the record).
	@RequestMapping(value = "/sentToKafkaTopic", method = RequestMethod.POST)
	public String sentToKafkaTopic(@RequestBody String jsonbody) {
		producer.send(new ProducerRecord<String, String>("test", String.valueOf(Math.random() * 10), jsonbody));
		// this.template.convertAndSend("/topic/currentTput", currentTput);
		return "OK";
	}

	// @RequestMapping(method=RequestMethod.POST)
	// public Double parseJson(@RequestBody String jsonbody){
	// Map<String, Object> JsonMap = Jparser.parseMap(jsonbody);
	// currentTput = (Double) JsonMap.get("currentvideoThroughput");
	//// currentplayUnit = JsonMap.get("currentPlayUnit").toString();
	//// currentbufferedUnit = JsonMap.get("currentBufferedUnit").toString();
	//// bufferVariation = (Integer)
	// JsonMap.get("incrementalBufferSizeVariation");
	//// currentplayStatus = JsonMap.get("currentPlayStatus").toString();
	//// //TimeUnit.MILLISECONDS.sleep(1900);
	//// TimeUnit.MILLISECONDS.sleep(900);
	//// currentTput = currentTputDefault;
	//// currentplayUnit = currentplayUnitDefault;
	//// currentbufferedUnit = currentbufferedUnitDefault;
	//// currentplayStatus = currentplayStatusDefault;
	//// bufferVariation = bufferVariationDefault;
	// this.template.convertAndSend("/topic/currentTput", currentTput);
	// return currentTput;
	// }
	@RequestMapping(value = "/getTput", method = RequestMethod.GET)
	public double getTput() {
		return currentTput;
	}

	@RequestMapping(value = "/getUnit", method = RequestMethod.GET)
	public String getUnit() {
		return currentbufferedUnit + "," + currentplayUnit + "," + currentplayStatus;
	}

	@RequestMapping(value = "/getBufferVariation", method = RequestMethod.GET)
	public int getBufferVariation() {
		return bufferVariation;
	}
}
