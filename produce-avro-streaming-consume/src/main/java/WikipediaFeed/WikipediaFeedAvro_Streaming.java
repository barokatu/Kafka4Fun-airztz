package WikipediaFeed;

import WikipediaFeed.WikiFeed;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

//java -cp produce-avro-streaming-consume/target/produce-avro-streaming-consume-1.0-SNAPSHOT.jar WikipediaFeed.WikipediaFeedAvro_Streaming

public class WikipediaFeedAvro_Streaming {

    static final String WIKIPEDIA_FEED = "WikipediaFeed";
    static final String WIKIPEDIA_STATS = "WikipediaStats";

    public static void main(final String[] args) throws Exception {
        final String bootstrapServers = args.length > 0 ? args[0] : "hdfsHA5:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://hdfsHA5:8081";
        final KafkaStreams streams = buildWikipediaFeed(
                bootstrapServers,
                schemaRegistryUrl,
                "/tmp/kafka-streams");
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static KafkaStreams buildWikipediaFeed(final String bootstrapServers,
                                           final String schemaRegistryUrl,
                                           final String stateDir) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "WikipediaFeed-avro");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "WikipediaFeed-avro-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        // read the source stream
        final KStream<String, WikiFeed> feeds = builder.stream(WIKIPEDIA_FEED);

        // aggregate the new feed counts of by user
        final KTable<String, Long> aggregated = feeds
                // filter out old feeds
                .filter((dummy, value) -> value.getIsNew())
                // map the user id as key
                .map((key, value) -> new KeyValue<>(value.getUser(), value))
                // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupByKey()
                .count();

        // write to the result topic, need to override serdes
        aggregated.toStream().to(WIKIPEDIA_STATS, Produced.with(stringSerde, longSerde));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }
}
