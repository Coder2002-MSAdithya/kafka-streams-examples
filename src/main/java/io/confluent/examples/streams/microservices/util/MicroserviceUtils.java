package io.confluent.examples.streams.microservices.util;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.OrderValidation;
import io.confluent.examples.streams.avro.microservices.OrderValidationResult;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.Service;
import io.confluent.examples.streams.microservices.domain.Schemas;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.message.AddClientPrivsResponseData;
import org.apache.kafka.common.message.AddTagResponseData;
import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.message.GrantCapResponseData;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.difc.DifcPrivilegeRequestHandler;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.core.Response;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class MicroserviceUtils {

  private static final Logger log = LoggerFactory.getLogger(MicroserviceUtils.class);

  /**
   * Stream/tombstone filter: DIFC may replace unauthorized payloads with null values;
   * compacted topics and window evictions also emit null-valued records.
   */
  public static <K, V> boolean isNotTombstone(final K ignored, final V value) {
    return value != null;
  }

  public static boolean isLiveCreatedOrder(final String ignored, final Order order) {
    return isLiveCreatedOrder(order);
  }

  public static boolean isLiveCreatedOrder(final Order order) {
    return order != null && OrderState.CREATED.equals(order.getState());
  }

  public static boolean isLiveFailedValidation(final String ignored, final OrderValidation validation) {
    return validation != null && OrderValidationResult.FAIL.equals(validation.getValidationResult());
  }

  /**
   * Log every record read from {@code orders} before tombstone/state filters run.
   */
  public static void logOrderIntake(final String serviceName, final String orderKey, final Order order) {
    if (order == null) {
      System.out.printf(
          "[%s] Order key=%s status=tombstone/redacted reason=null value (DIFC payload strip or compact delete)%n",
          serviceName, orderKey);
      return;
    }
    if (!OrderState.CREATED.equals(order.getState())) {
      System.out.printf(
          "[%s] Order key=%s status=skipped reason=state is %s (only CREATED is validated)%n",
          serviceName, orderKey, order.getState());
      return;
    }
    System.out.printf(
        "[%s] Order key=%s status=accepted for validation id=%s customer=%s product=%s quantity=%s price=%s%n",
        serviceName,
        orderKey,
        order.getId(),
        order.getCustomerId(),
        order.getProduct(),
        order.getQuantity(),
        order.getPrice());
  }

  /**
   * Log every record read from {@code order-validations} before tombstone filters run.
   */
  public static void logValidationIntake(final String serviceName,
                                         final String validationKey,
                                         final OrderValidation validation) {
    if (validation == null) {
      System.out.printf(
          "[%s] Validation key=%s status=tombstone/redacted reason=null value%n",
          serviceName, validationKey);
      return;
    }
    System.out.printf(
        "[%s] Validation key=%s status=received orderId=%s type=%s result=%s%n",
        serviceName,
        validationKey,
        validation.getOrderId(),
        validation.getCheckType(),
        validation.getValidationResult());
  }

  public static void logValidationDecision(final String serviceName,
                                           final String orderId,
                                           final String checkType,
                                           final OrderValidationResult result,
                                           final String reason) {
    System.out.printf(
        "[%s] Validation decision orderId=%s type=%s result=%s reason=%s%n",
        serviceName, orderId, checkType, result, reason);
  }

  public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  public static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

  public static Properties buildPropertiesFromConfigFile(final String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }
    final Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      properties.load(inputStream);
    }
    return properties;
  }

  public static Properties baseStreamsConfig(final String bootstrapServers,
                                             final String stateDir,
                                             final String appId,
                                             final Properties defaultConfig) {
    return baseStreamsConfig(bootstrapServers, stateDir, appId, false, defaultConfig);
  }

  public static Properties baseStreamsConfigEOS(final String bootstrapServers,
                                                final String stateDir,
                                                final String appId,
                                                final Properties defaultConfig) {
    return baseStreamsConfig(bootstrapServers, stateDir, appId, true, defaultConfig);
  }

  public static Properties baseStreamsConfig(final String bootstrapServers,
                                             final String stateDir,
                                             final String appId,
                                             final boolean enableEOS,
                                             final Properties defaultConfig) {

    final Properties config = new Properties();
    config.putAll(defaultConfig);
    // Workaround for a known issue with RocksDB in environments where you have only 1 cpu core.
    config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    config.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
            enableEOS ? "exactly_once" : "at_least_once");
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1); //commit as fast as possible
    config.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 30000);
    return config;
  }

  public static class CustomRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(final String storeName, final Options options,
        final Map<String, Object> configs) {
      // Workaround: We must ensure that the parallelism is set to >= 2.  There seems to be a known
      // issue with RocksDB where explicitly setting the parallelism to 1 causes issues (even though
      // 1 seems to be RocksDB's default for this configuration).
      final int compactionParallelism = Math.max(Runtime.getRuntime().availableProcessors(), 2);
      // Set number of compaction threads (but not flush threads).
      options.setIncreaseParallelism(compactionParallelism);
    }

    @Override
    public void close(final String storeName, final Options options) {

    }
  }

  //Streams doesn't provide an Enum serdes so just create one here.
  public static final class ProductTypeSerde implements Serde<Product> {

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Product> serializer() {
      return new Serializer<Product>() {
        @Override
        public void configure(final Map<String, ?> map, final boolean b) {
        }

        @Override
        public byte[] serialize(final String topic, final Product pt) {
          return pt.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void close() {
        }
      };
    }

    @Override
    public Deserializer<Product> deserializer() {
      return new Deserializer<Product>() {
        @Override
        public void configure(final Map<String, ?> map, final boolean b) {
        }

        @Override
        public Product deserialize(final String topic, final byte[] bytes) {
          return Product.valueOf(new String(bytes, StandardCharsets.UTF_8));
        }

        @Override
        public void close() {
        }
      };
    }
  }

  public static void setTimeout(final long timeout, final AsyncResponse asyncResponse) {
    asyncResponse.setTimeout(timeout, TimeUnit.MILLISECONDS);
    asyncResponse.setTimeoutHandler(resp -> resp.resume(
        Response.status(Response.Status.GATEWAY_TIMEOUT)
            .entity("HTTP GET timed out after " + timeout + " ms\n")
            .build()));
  }

  public static Server startJetty(final int port, final Object binding) {
    final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");

    final Server jettyServer = new Server(port);
    jettyServer.setHandler(context);

    final ResourceConfig rc = new ResourceConfig();
    rc.register(binding);
    rc.register(JacksonFeature.class);

    final ServletContainer sc = new ServletContainer(rc);
    final ServletHolder holder = new ServletHolder(sc);
    context.addServlet(holder, "/*");

    try {
      jettyServer.start();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    log.info("Listening on " + jettyServer.getURI());
    return jettyServer;
  }

  public static <T> KafkaProducer startProducer(final String bootstrapServers,
                                                final Schemas.Topic<String, T> topic,
                                                final Properties defaultConfig) {
    final Properties producerConfig = new Properties();
    producerConfig.putAll(defaultConfig);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "order-sender");

    return new KafkaProducer<>(producerConfig,
        topic.keySerde().serializer(),
        topic.valueSerde().serializer());
  }

  private static void requireDifcOk(final int errorCode,
                                    final String operation,
                                    final String detail) {
    if (errorCode != 0) {
      throw new IllegalStateException(
          operation + " failed with errorCode=" + errorCode + (detail.isEmpty() ? "" : ": " + detail));
    }
  }

  public static void registerDifcClient(final String serviceName,
                                        final String bootstrapServers,
                                        final Properties defaultConfig) {
    final Properties producerConfig = new Properties();
    producerConfig.putAll(defaultConfig);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, serviceName + "-difc-registrar");

    try (KafkaProducer<String, String> registrar = new KafkaProducer<>(
        producerConfig,
        new StringSerializer(),
        new StringSerializer())) {
      final RegisterClientResponseData response = registrar.registerClient();
      System.out.printf("[DIFC] registerClient service=%s errorCode=%d message=%s%n",
          serviceName,
          response.errorCode(),
          response.errorMessage());
      requireDifcOk(response.errorCode(), "registerClient(" + serviceName + ")", response.errorMessage());
    }
  }

  /** Tag name set for {@link org.apache.kafka.streams.kstream.KStream#addTags(Set)} / {@code declassifyTags}. */
  public static Set<String> difcTagSet(final String... tagNames) {
    if (tagNames == null || tagNames.length == 0) {
      return Collections.emptySet();
    }
    if (tagNames.length == 1) {
      return Collections.singleton(tagNames[0]);
    }
    return Set.of(tagNames);
  }

  /**
   * Add a tag to this Streams client's DIFC label (required for FETCH/produce tag union).
   */
  public static void addDifcTagToClientLabel(final String serviceName,
                                            final KafkaStreams streams,
                                            final String tagName) {
    final AddTagResponseData labelResponse = streams.addTag(tagName);
    System.out.printf(
        "[DIFC] addTagToLabel service=%s tag=%s errorCode=%d message=%s%n",
        serviceName,
        tagName,
        labelResponse.errorCode(),
        labelResponse.errorMessage());
    if (labelResponse.errorCode() != 0) {
      throw new IllegalStateException(
          "addTagToLabel(" + serviceName + ", " + tagName + ") failed with errorCode="
              + labelResponse.errorCode() + ": " + labelResponse.errorMessage());
    }
  }

  /**
   * Send {@code GRANT_CAP} via the running {@link KafkaStreams} app's shared producer.
   */
  public static void requestDifcGrantCap(final String serviceName,
                                        final KafkaStreams streams,
                                        final String tagName,
                                        final Capability capability) {
    final GrantCapResponseData response = streams.requestGrantCap(tagName, capability);
    System.out.printf(
        "[DIFC] requestGrantCap service=%s tag=%s capability=%s errorCode=%d message=%s%n",
        serviceName,
        tagName,
        capability,
        response.errorCode(),
        response.errorMessage());
    requireDifcOk(
        response.errorCode(),
        "requestGrantCap(" + serviceName + ", " + tagName + ", " + capability + ")",
        response.errorMessage());
  }

  /**
   * Request {@code CAN_ADD}/{@code CAN_REMOVE} on a tag owned by another client (no label change).
   */
  public static void requestDifcGrantCapOnly(final String serviceName,
                                             final KafkaStreams streams,
                                             final String tagName) {
    requestDifcGrantCap(serviceName, streams, tagName, Capability.CAN_ADD);
    requestDifcGrantCap(serviceName, streams, tagName, Capability.CAN_REMOVE);
  }

  /**
   * Request {@code CAN_ADD}/{@code CAN_REMOVE} on a shared tag, then add the tag to this
   * client's label. FETCH redaction uses label tags ({@code canClientReceive}), not
   * {@code canAdd} alone — {@link KafkaStreams#addTag} is required to read tagged records.
   */
  public static void requestDifcGrantCapAddAndRemove(final String serviceName,
                                                     final KafkaStreams streams,
                                                     final String tagName) {
    requestDifcGrantCapOnly(serviceName, streams, tagName);
    // Grants are applied asynchronously by the tag owner; retry until label add succeeds.
    final int maxAttempts = 15;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        addDifcTagToClientLabel(serviceName, streams, tagName);
        return;
      } catch (final IllegalStateException e) {
        if (attempt == maxAttempts) {
          throw e;
        }
        try {
          Thread.sleep(2000L);
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("Interrupted waiting for DIFC grant on tag " + tagName, ie);
        }
      }
    }
  }

  public static Capability capabilityFromPollResponse(final byte capability) {
    return switch (capability) {
      case 0 -> Capability.CAN_ADD;
      case 1 -> Capability.CAN_REMOVE;
      default -> throw new IllegalArgumentException("Unknown DIFC capability code: " + capability);
    };
  }

  /**
   * Grant a pending capability request (tag owner approves requester via {@code ADD_CLIENT_PRIVS}).
   */
  public static void grantPendingPrivilegeRequest(final String serviceName,
                                                  final KafkaStreams streams,
                                                  final PollPrivsReqResponseData pending) {
    if (pending == null || pending.capability() < 0) {
      return;
    }
    final String tagName = pending.tagName();
    final String requester = pending.requesterClientId();
    if (tagName == null || tagName.isEmpty() || requester == null || requester.isEmpty()) {
      return;
    }
    final Capability capability = capabilityFromPollResponse(pending.capability());
    final AddClientPrivsResponseData response =
        streams.addClientPrivs(requester, tagName, capability);
    System.out.printf(
        "[DIFC] grantPrivilege service=%s requester=%s tag=%s capability=%s errorCode=%d message=%s%n",
        serviceName,
        requester,
        tagName,
        capability,
        response.errorCode(),
        response.errorMessage());
  }

  public static DifcPrivilegeRequestHandler autoGrantPrivilegeRequests(final String serviceName,
                                                                      final KafkaStreams streams) {
    return pending -> grantPendingPrivilegeRequest(serviceName, streams, pending);
  }

  /**
   * {@link KafkaStreams} that auto-approves incoming {@code GRANT_CAP} requests (tag owners only).
   */
  public static KafkaStreams kafkaStreamsWithAutoGrant(final Topology topology,
                                                       final Properties props,
                                                       final String serviceName) {
    final KafkaStreams[] holder = new KafkaStreams[1];
    holder[0] = new KafkaStreams(
        topology,
        props,
        pending -> grantPendingPrivilegeRequest(serviceName, holder[0], pending));
    return holder[0];
  }

  public static void createDifcTag(final String serviceName,
                                   final String tagName,
                                   final String bootstrapServers,
                                   final Properties defaultConfig) {
    final Properties producerConfig = new Properties();
    producerConfig.putAll(defaultConfig);
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, serviceName + "-difc-tag-creator");

    try (KafkaProducer<String, String> registrar = new KafkaProducer<>(
        producerConfig,
        new StringSerializer(),
        new StringSerializer())) {
      final CreateTagResponseData response = registrar.createTag(tagName);
      System.out.printf("[DIFC] createTag service=%s tag=%s errorCode=%d message=%s%n",
          serviceName,
          tagName,
          response.errorCode(),
          response.errorMessage());
      requireDifcOk(
          response.errorCode(),
          "createTag(" + serviceName + ", " + tagName + ")",
          response.errorMessage());
    }
  }

  public static void addShutdownHookAndBlock(final Service service) throws InterruptedException {
    Thread.currentThread().setUncaughtExceptionHandler((t, e) -> service.stop());
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        service.stop();
      } catch (final Exception ignored) {
      }
    }));
    Thread.currentThread().join();
  }
}
