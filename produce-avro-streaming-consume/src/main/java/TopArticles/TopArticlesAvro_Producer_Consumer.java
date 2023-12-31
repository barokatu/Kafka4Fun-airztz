package TopArticles;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;
public class TopArticlesAvro_Producer_Consumer {
    public static void main(String[] args) throws IOException {
        final String bootstrapServers = args.length > 0 ? args[0] : "hdfsHA5:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://hdfsHA5:8081";
        //produceInputs_GenericRecord(bootstrapServers, schemaRegistryUrl);
        produceInputs_SpecificRecord(bootstrapServers, schemaRegistryUrl);
        consumeOutput(bootstrapServers, schemaRegistryUrl);
    }

    private static void produceInputs_GenericRecord(String bootstrapServers, String schemaRegistryUrl) throws IOException {
        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam",
                "lauren", "joseph"};
        final String[] industries = {"engineering", "telco", "finance", "health", "science"};
        final String[] pages = {"index.html", "news.html", "contact.html", "about.html", "stuff.html"};

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);

        final GenericRecordBuilder pageViewBuilder =
                new GenericRecordBuilder(loadSchema("pageview.avsc"));

        final Random random = new Random();
        for (String user : users) {
            pageViewBuilder.set("industry", industries[random.nextInt(industries.length)]);
            pageViewBuilder.set("flags", "ARTICLE");
            // For each user generate some page views
            IntStream.range(0, random.nextInt(10))
                    .mapToObj(value -> {
                        pageViewBuilder.set("user", user);
                        pageViewBuilder.set("page", pages[random.nextInt(pages.length)]);
                        return pageViewBuilder.build();
                    })
                    .forEach(record ->
                            //System.out.println(record)
                            producer.send(new ProducerRecord<>(TopArticlesAvro_Streaming.PAGE_VIEWS, null, record))
            );
        }
        producer.flush();
        producer.close();
    }

    private static void produceInputs_SpecificRecord(String bootstrapServers, String schemaRegistryUrl) throws IOException {
        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam",
                "lauren", "joseph"};
        final String[] industries = {"engineering", "telco", "finance", "health", "science"};
        final String[] pages = {"index.html", "news.html", "contact.html", "about.html", "stuff.html"};

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        final KafkaProducer<String, PageView> producer = new KafkaProducer<>(props);

        final Random random = new Random();
        for (String user : users) {
           String industry = industries[random.nextInt(industries.length)];
            // For each user generate some page views
            IntStream.range(0, random.nextInt(10))
                    .mapToObj(value ->
                            new PageView(user,pages[random.nextInt(pages.length)],industry,"ARTICLE")
                    )
                    .forEach(record ->
                                    //System.out.println(record)
                            producer.send(new ProducerRecord<>(TopArticlesAvro_Streaming.PAGE_VIEWS, null, record))
                    );
        }
        producer.flush();
        producer.close();
    }

    private static void consumeOutput(String bootstrapServers, String schemaRegistryUrl) {
        final Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        consumerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                "top-articles-lambda-example-consumer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final Deserializer<Windowed<String>> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
        final KafkaConsumer<Windowed<String>, String> consumer = new KafkaConsumer<>(consumerProperties,
                windowedDeserializer,
                Serdes.String().deserializer());

        consumer.subscribe(Collections.singleton(TopArticlesAvro_Streaming.TOP_NEWS_PER_INDUSTRY_TOPIC));
        while (true) {
            ConsumerRecords<Windowed<String>, String> consumerRecords = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<Windowed<String>, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.key().key() + "@" + consumerRecord.key().window().start() +  "=" + consumerRecord.value());
            }
        }
    }

    static Schema loadSchema(String name) throws IOException {
        try (InputStream input = TopArticlesAvro_Streaming.class.getClassLoader()
                .getResourceAsStream("avro/" + name)) {
            return new Schema.Parser().parse(input);
        }
    }
}
