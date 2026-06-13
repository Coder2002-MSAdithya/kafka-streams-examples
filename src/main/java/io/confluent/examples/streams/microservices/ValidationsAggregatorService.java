package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.examples.streams.avro.microservices.OrderState.VALIDATED;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * A simple service which listens to validation results from each of the Validation
 * services and aggregates them by order ID, triggering a pass or fail based on whether
 * all rules pass or not.
 */
public class ValidationsAggregatorService implements Service {

  private static final Logger log = LoggerFactory.getLogger(ValidationsAggregatorService.class);
  private final String SERVICE_APP_ID = getClass().getSimpleName();
  /** Validation tags on {@code order-validations} from Fraud/Inventory/OrderDetails. */
  private static final String DIFC_TAG_FRAUD = "fraud";
  private static final String DIFC_TAG_INV_VALID = "inv-valid";
  private static final String DIFC_TAG_ORDER_VALID = "order-valid";
  /** Tags stripped from {@code orders} output (label + record union minus declassify header). */
  private static final Set<String> DIFC_OUTPUT_DECLASSIFY_TAGS = difcTagSet(
      DIFC_TAG_FRAUD, DIFC_TAG_INV_VALID, DIFC_TAG_ORDER_VALID, "order");
  /** Tag on {@code orders} from OrdersService; needed to read/join tagged orders. */
  private static final String DIFC_ORDER_TAG = "order";
  private final Consumed<String, OrderValidation> serdes1 = Consumed
      .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde());
  private final Consumed<String, Order> serdes2 = Consumed.with(ORDERS.keySerde(),
      ORDERS.valueSerde());
  private final Grouped<String, OrderValidation> serdes3 = Grouped
      .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde());
  private final StreamJoined<String, Long, Order> serdes4 = StreamJoined
      .with(ORDERS.keySerde(), Serdes.Long(), ORDERS.valueSerde());
  private final Produced<String, Order> serdes5 = Produced
      .with(ORDERS.keySerde(), ORDERS.valueSerde());
  private final Grouped<String, Order> serdes6 = Grouped
      .with(ORDERS.keySerde(), ORDERS.valueSerde());
  private final StreamJoined<String, OrderValidation, Order> serdes7 = StreamJoined
      .with(ORDERS.keySerde(), ORDER_VALIDATIONS.valueSerde(), ORDERS.valueSerde());

  private KafkaStreams streams;

  @Override
  public void start(final String bootstrapServers,
                    final String stateDir,
                    final Properties defaultConfig) {
    final CountDownLatch startLatch = new CountDownLatch(1);
    streams = aggregateOrderValidations(bootstrapServers, stateDir, defaultConfig);

    streams.setStateListener((newState, oldState) -> {
      if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
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
    setupDifcPrivileges(streams);

    log.info("Started Service " + getClass().getSimpleName());
  }

  private void setupDifcPrivileges(final KafkaStreams streams) {
    requestDifcGrantCapAddAndRemove(SERVICE_APP_ID, streams, DIFC_TAG_FRAUD);
    requestDifcGrantCapAddAndRemove(SERVICE_APP_ID, streams, DIFC_TAG_INV_VALID);
    requestDifcGrantCapAddAndRemove(SERVICE_APP_ID, streams, DIFC_TAG_ORDER_VALID);
    requestDifcGrantCapAddAndRemove(SERVICE_APP_ID, streams, DIFC_ORDER_TAG);
  }

  private KafkaStreams aggregateOrderValidations(
      final String bootstrapServers,
      final String stateDir,
      final Properties defaultConfig) {
    final int numberOfRules = 3; //TODO put into a KTable to make dynamically configurable

    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, OrderValidation> validations = builder
        .stream(ORDER_VALIDATIONS.name(), serdes1)
        .peek((id, validation) -> logValidationIntake(SERVICE_APP_ID, id, validation))
        .filter((id, validation) -> isNotTombstone(id, validation));
    final KStream<String, Order> orders = builder
        .stream(ORDERS.name(), serdes2)
        .peek((id, order) -> logOrderIntake(SERVICE_APP_ID, id, order))
        .filter((id, order) -> isLiveCreatedOrder(id, order));

    //If all rules pass then validate the order
    final KStream<String, Order> validatedOrders = validations
        .groupByKey(serdes3)
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(5)))
        .aggregate(
            () -> 0L,
            (id, result, total) -> PASS.equals(result.getValidationResult()) ? total + 1 : total,
            (k, a, b) -> b == null ? a : b, //include a merger as we're using session windows.
            Materialized.with(null, Serdes.Long())
        )
        //get rid of window
        .toStream((windowedKey, total) -> windowedKey.key())
        //When elements are evicted from a session window they create delete events. Filter these.
        .filter((k1, v) -> v != null)
        //only include results were all rules passed validation
        .filter((k, total) -> total >= numberOfRules)
        //Join back to orders
        .join(orders, (id, order) -> {
                final Order validatedOrder = newBuilder(order).setState(VALIDATED).build();
                System.out.printf(
                    "[%s] Aggregation decision orderId=%s state=%s reason=all %d validators returned PASS within join window%n",
                    SERVICE_APP_ID,
                    validatedOrder.getId(),
                    validatedOrder.getState(),
                    numberOfRules);
                return validatedOrder;
            }, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)), serdes4);

    //If any rule fails then fail the order
    final KStream<String, Order> failedOrders = validations.filter((id, rule) -> isLiveFailedValidation(id, rule))
        .join(orders, (id, order) -> {
                final Order failedOrder = newBuilder(order).setState(OrderState.FAILED).build();
                System.out.printf(
                    "[%s] Aggregation decision orderId=%s state=%s reason=at least one validator returned FAIL (fraud, inventory, or order-details)%n",
                    SERVICE_APP_ID,
                    failedOrder.getId(),
                    failedOrder.getState());
                return failedOrder;
            }, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)), serdes7)
        //there could be multiple failed rules for each order so collapse to a single order
        .groupByKey(serdes6)
        .reduce((order, v1) -> order)
        .toStream();

    validatedOrders.merge(failedOrders)
        .declassifyTags(DIFC_OUTPUT_DECLASSIFY_TAGS)
        .to(ORDERS.name(), serdes5);

    return new KafkaStreams(
        builder.build(),
        baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig));
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
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
      final HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Validator Aggregator Service", opts);
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

    final ValidationsAggregatorService service = new ValidationsAggregatorService();

    service.start(
            cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
            cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
            defaultConfig);
    addShutdownHookAndBlock(service);
  }
}