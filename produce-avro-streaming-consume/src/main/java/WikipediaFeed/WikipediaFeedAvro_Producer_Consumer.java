package WikipediaFeed;

import WikipediaFeed.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;


//java -cp produce-avro-streaming-consume/target/produce-avro-streaming-consume-1.0-SNAPSHOT.jar WikipediaFeedAvro_Producer_Consumer

public class WikipediaFeedAvro_Producer_Consumer {

    public static void main(final String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "hdfsHA5:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://hdfsHA5:8081";
        produceInputs(bootstrapServers, schemaRegistryUrl);
        consumeOutput(bootstrapServers, schemaRegistryUrl);
    }

    private static void produceInputs(String bootstrapServers, String schemaRegistryUrl) throws IOException {
        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam",
                "lauren", "joseph"};

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //Use simple StringSerializer for key, it can be KafkaAvroSerializer but not necessary in this case
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        //Here we use Kafka Avro Serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final KafkaProducer<String, WikiFeed> producer = new KafkaProducer<>(props);

        final Random random = new Random();

        IntStream.range(0, random.nextInt(100))
                .mapToObj(value -> new WikiFeed(users[random.nextInt(users.length)], true, "content"))
                .forEach(record -> producer.send(new ProducerRecord<>(WikipediaFeedAvro_Streaming.WIKIPEDIA_FEED, null, record)));

        producer.flush();
    }

    private static void consumeOutput(String bootstrapServers, String schemaRegistryUrl) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-feed-example-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProperties,
                new StringDeserializer(),
                new LongDeserializer());

        consumer.subscribe(Collections.singleton(WikipediaFeedAvro_Streaming.WIKIPEDIA_STATS));
        while (true) {
            final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key() + "=" + consumerRecord.value());
            }
        }
    }


}
