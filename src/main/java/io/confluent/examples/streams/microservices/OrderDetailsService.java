package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.ORDER_DETAILS_CHECK;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.microservices.domain.Schemas;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.Capability;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the details of each order (quantity, price, product).
 * <p>
 * Implemented with Kafka Streams (same pattern as {@link FraudService}) so DIFC operations
 * use the shared Streams producer via {@link KafkaStreams#requestGrantCap}.
 */
public class OrderDetailsService implements Service {

  private static final Logger log = LoggerFactory.getLogger(OrderDetailsService.class);
  private static final String SERVICE_APP_ID = "OrderDetailsService";
  private static final String DIFC_ORDER_TAG = "order";
  private static final String DIFC_VALIDATION_TAG = "order-valid";

  private KafkaStreams streams;

  @Override
  public void start(final String bootstrapServers,
                    final String stateDir,
                    final Properties defaultConfig) {
    streams = processStreams(bootstrapServers, stateDir, defaultConfig);

    final CountDownLatch startLatch = new CountDownLatch(1);
    streams.setStateListener((newState, oldState) -> {
      if (newState == State.RUNNING && oldState != State.RUNNING) {
        startLatch.countDown();
      }
    });
    streams.start();

    try {
      if (!startLatch.await(60, TimeUnit.SECONDS)) {
        throw new RuntimeException("Streams never finished rebalancing on startup");
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    registerDifcClient(SERVICE_APP_ID, streams);
    createDifcTag(SERVICE_APP_ID, streams, DIFC_VALIDATION_TAG);
    addDifcTagToClientLabel(SERVICE_APP_ID, streams, DIFC_VALIDATION_TAG);
    requestDifcGrantCapAddAndRemove(SERVICE_APP_ID, streams, DIFC_ORDER_TAG);

    log.info("Started Service " + getClass().getSimpleName());
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
    log.info(getClass().getSimpleName() + " was stopped");
  }

  private KafkaStreams processStreams(final String bootstrapServers,
                                      final String stateDir,
                                      final Properties defaultConfig) {
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, Order> orders = builder
        .stream(Topics.ORDERS.name(), Consumed.with(Topics.ORDERS.keySerde(), Topics.ORDERS.valueSerde()))
        .peek((id, order) -> logOrderIntake(SERVICE_APP_ID, id, order))
        .filter((id, order) -> isLiveCreatedOrder(id, order));

    orders
        .mapValues(order -> {
          final String failReason = validationFailReason(order);
          final OrderValidationResult validationResult = failReason == null ? PASS : FAIL;
          final OrderValidation validation =
              new OrderValidation(order.getId(), ORDER_DETAILS_CHECK, validationResult);
          logValidationDecision(
              SERVICE_APP_ID,
              order.getId(),
              ORDER_DETAILS_CHECK.name(),
              validationResult,
              failReason == null
                  ? "order details valid (quantity>=0, price>=0, product present)"
                  : failReason);
          return validation;
        })
        .declassifyTags(difcTagSet(DIFC_ORDER_TAG))
        .addTags(difcTagSet(DIFC_VALIDATION_TAG))
        .to(Topics.ORDER_VALIDATIONS.name(),
            Produced.with(Topics.ORDER_VALIDATIONS.keySerde(), Topics.ORDER_VALIDATIONS.valueSerde()));

    return kafkaStreamsWithAutoGrant(
        builder.build(),
        baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig),
        SERVICE_APP_ID);
  }

  /**
   * @return human-readable failure reason, or {@code null} if the order passes validation
   */
  static String validationFailReason(final Order order) {
    if (order.getQuantity() < 0) {
      return "quantity is negative: " + order.getQuantity();
    }
    if (order.getPrice() < 0) {
      return "price is negative: " + order.getPrice();
    }
    if (order.getProduct() == null) {
      return "product is missing";
    }
    return null;
  }

  public static void main(final String[] args) throws Exception {
    final Options opts = new Options();
    opts.addOption(Option.builder("b")
            .longOpt("bootstrap-servers")
            .hasArg()
            .desc("Kafka cluster bootstrap server string (ex: broker:9092)")
            .build());
    opts.addOption(Option.builder("s")
            .longOpt("schema-registry")
            .hasArg()
            .desc("Schema Registry URL")
            .build());
    opts.addOption(Option.builder("c")
            .longOpt("config-file")
            .hasArg()
            .desc("Java properties file with configurations for Kafka Clients")
            .build());
    opts.addOption(Option.builder("t")
            .longOpt("state-dir")
            .hasArg()
            .desc("The directory for state storage")
            .build());
    opts.addOption(Option.builder("h").longOpt("help").hasArg(false).desc("Show usage information").build());

    final CommandLine cl = new DefaultParser().parse(opts, args);

    if (cl.hasOption("h")) {
      new HelpFormatter().printHelp("Order Details Service", opts);
      return;
    }
    final Properties defaultConfig = Optional.ofNullable(cl.getOptionValue("config-file", (String) null))
            .map(path -> {
              try {
                return buildPropertiesFromConfigFile(path);
              } catch (final IOException e) {
                throw new RuntimeException(e);
              }
            })
            .orElse(new Properties());

    final String schemaRegistryUrl = cl.getOptionValue("schema-registry", DEFAULT_SCHEMA_REGISTRY_URL);
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    Schemas.configureSerdes(defaultConfig);

    final OrderDetailsService service = new OrderDetailsService();
    service.start(
            cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
            cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
            defaultConfig);
    addShutdownHookAndBlock(service);
  }
}
