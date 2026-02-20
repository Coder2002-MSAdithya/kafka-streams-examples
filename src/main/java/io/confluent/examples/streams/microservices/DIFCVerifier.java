package io.confluent.examples.streams.microservices;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.difc.DifcSyncFacade;
import org.apache.kafka.streams.internals.DifcStreamsRuntime;

import java.util.Properties;

/**
 * Simple program to verify DIFC Kafka version by directly calling dummy methods
 */
public class DIFCVerifier {

    public static void main(final String[] args) {
        System.out.println("=================================");
        System.out.println("DIFC Kafka Verification Program");
        System.out.println("=================================");

        try {
            // Create minimal config
            final Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "difc-verifier");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

            // Add these to prevent the streams instance from running indefinitely
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1); // Single thread
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable caching

            // Create a topology with a dummy source topic
            final StreamsBuilder builder = new StreamsBuilder();

            // Add a dummy source topic - this topic doesn't need to exist
            // Using a pattern that won't match any real topics
            builder.stream(java.util.regex.Pattern.compile("^__difc_verifier_dummy_topic_\\d+$"));

            final Topology topology = builder.build();

            // Print the topology for debugging
            System.out.println("\nTopology description:");
            System.out.println(topology.describe());

            // Create KafkaStreams object
            final KafkaStreams streams = new KafkaStreams(topology, props);

            // Start the streams instance
            streams.start();

            // Give it a moment to initialize
            Thread.sleep(2000);

            // DIRECTLY CALL THE DUMMY METHODS
            System.out.println("\n1. Calling getDIFCVersion():");
            final String version = streams.getDIFCVersion();
            System.out.println("   Result: \"" + version + "\"");

            System.out.println("\n2. Calling isDIFCEnabled():");
            final boolean enabled = streams.isDIFCEnabled();
            System.out.println("   Result: " + enabled);

            System.out.println("\n3. Combined result:");
            if (enabled && "DIFC-Enabled-Kafka-4.0.0".equals(version)) {
                System.out.println("   ✅✅✅ SUCCESS! Using DIFC-enabled Kafka 4.0.0");
            } else {
                System.out.println("   ❌ Using standard Kafka without DIFC");
            }

            // Clean up
            streams.close();

        } catch (final NoSuchMethodError e) {
            System.out.println("\n❌❌❌ ERROR: Dummy methods not found!");
            System.out.println("The project is NOT using your modified DIFC Kafka version.");
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        } catch (final Exception e) {
            System.out.println("\n❌ Error occurred:");
            e.printStackTrace();
        }
    }
}