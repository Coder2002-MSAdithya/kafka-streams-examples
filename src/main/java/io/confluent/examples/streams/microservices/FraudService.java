package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValue;
import io.confluent.examples.streams.microservices.domain.Schemas;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.Capability;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KafkaStreams.State;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.FAIL;
import static io.confluent.examples.streams.avro.microservices.OrderValidationResult.PASS;
import static io.confluent.examples.streams.avro.microservices.OrderValidationType.FRAUD_CHECK;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDER_VALIDATIONS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;


/**
 * This service searches for potentially fraudulent transactions by calculating the total value of
 * orders for a customer within a time period, then checks to see if this is over a configured
 * limit. <p> i.e. if(SUM(order.value, 5Mins) > $5000) GroupBy customer -> Fail(orderId) else
 * Pass(orderId)
 */
public class FraudService implements Service {

  private static final Logger log = LoggerFactory.getLogger(FraudService.class);
  private final String SERVICE_APP_ID = getClass().getSimpleName();

  private static final int FRAUD_LIMIT = 2000;
  /** Tag on {@code orders} records produced by OrdersService; fraud-svc needs CAN_ADD on the label. */
  private static final String DIFC_ORDER_TAG = "order";
  /** Tag owned by this service; attached to {@code order-validations} output via {@code addTags}. */
  private static final String DIFC_VALIDATION_TAG = "fraud";
  private KafkaStreams streams;

  @Override
  public void start(final String bootstrapServers,
                    final String stateDir,
                    final Properties defaultConfig) {
    streams = processStreams(bootstrapServers, stateDir, defaultConfig);
    final CountDownLatch startLatch = new CountDownLatch(1);
    streams.setStateListener((newState, oldState) -> {
      if (newState == State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
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

  private KafkaStreams processStreams(final String bootstrapServers,
                                      final String stateDir,
                                      final Properties defaultConfig) {

    //Latch onto instances of the orders and inventory topics
    final StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, Order> orders = builder
        .stream(ORDERS.name(), Consumed.with(ORDERS.keySerde(), ORDERS.valueSerde()))
        .peek((id, order) -> logOrderIntake(SERVICE_APP_ID, id, order))
        .filter((id, order) -> isLiveCreatedOrder(id, order));

    //Create an aggregate of the total value by customer and hold it with the order. We use session windows to
    // detect periods of activity.
    final KTable<Windowed<Long>, OrderValue> aggregate = orders
        .groupBy((id, order) -> order.getCustomerId(), Grouped.with(Serdes.Long(), ORDERS.valueSerde()))
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofHours(1)))
        .aggregate(OrderValue::new,
            //Calculate running total for each customer within this window
            (custId, order, total) -> new OrderValue(order,
                total.getValue() + order.getQuantity() * order.getPrice()),
            (k, a, b) -> simpleMerge(a, b), //include a merger as we're using session windows.
            Materialized.with(null, Schemas.ORDER_VALUE_SERDE));

    //Ditch the windowing and rekey
    final KStream<String, OrderValue> ordersWithTotals = aggregate
        .toStream((windowedKey, orderValue) -> windowedKey.key())
        .filter((k, v) -> v != null)//When elements are evicted from a session window they create delete events. Filter these out.
        .selectKey((id, orderValue) -> orderValue.getOrder().getId());

    //Now branch the stream into two, for pass and fail, based on whether the windowed total is over Fraud Limit
    final Map<String, KStream<String, OrderValue>> forks = ordersWithTotals.split(Named.as("limit-"))
        .branch((id, orderValue) -> orderValue.getValue() >= FRAUD_LIMIT, Branched.as("above"))
        .branch((id, orderValue) -> orderValue.getValue() < FRAUD_LIMIT, Branched.as("below"))
        .noDefaultBranch();

    final KStream<String, OrderValidation> failedValidations = forks.get("limit-above").mapValues(orderValue -> {
      final OrderValidation validation = new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, FAIL);
      logValidationDecision(
          SERVICE_APP_ID,
          validation.getOrderId(),
          validation.getCheckType().name(),
          validation.getValidationResult(),
          String.format(
              "customer %s session order total %.2f >= fraud limit %d",
              orderValue.getOrder().getCustomerId(),
              orderValue.getValue(),
              FRAUD_LIMIT));
      return validation;
    });

    final KStream<String, OrderValidation> passedValidations = forks.get("limit-below").mapValues(orderValue -> {
      final OrderValidation validation = new OrderValidation(orderValue.getOrder().getId(), FRAUD_CHECK, PASS);
      logValidationDecision(
          SERVICE_APP_ID,
          validation.getOrderId(),
          validation.getCheckType().name(),
          validation.getValidationResult(),
          String.format(
              "customer %s session order total %.2f < fraud limit %d",
              orderValue.getOrder().getCustomerId(),
              orderValue.getValue(),
              FRAUD_LIMIT));
      return validation;
    });

    failedValidations.merge(passedValidations)
        .declassifyTags(difcTagSet(DIFC_ORDER_TAG))
        .addTags(difcTagSet(DIFC_VALIDATION_TAG))
        .to(ORDER_VALIDATIONS.name(), Produced
            .with(ORDER_VALIDATIONS.keySerde(), ORDER_VALIDATIONS.valueSerde()));

    //disable caching to ensure a complete aggregate changelog. This is a little trick we need to apply
    //as caching in Kafka Streams will conflate subsequent updates for the same key. Disabling caching ensures
    //we get a complete "changelog" from the aggregate(...) step above (i.e. every input event will have a
    //corresponding output event.
    final Properties props = baseStreamsConfig(bootstrapServers, stateDir, SERVICE_APP_ID, defaultConfig);
    props.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");

    return kafkaStreamsWithAutoGrant(builder.build(), props, SERVICE_APP_ID);
  }

  private OrderValue simpleMerge(final OrderValue a, final OrderValue b) {
    return new OrderValue(b.getOrder(), (a == null ? 0D : a.getValue()) + b.getValue());
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
      formatter.printHelp("Fraud Service", opts);
      return;
    }
    final FraudService service = new FraudService();
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

    service.start(
            cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS),
            cl.getOptionValue("state-dir", "/tmp/kafka-streams-examples"),
            defaultConfig);
    addShutdownHookAndBlock(service);
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

}
