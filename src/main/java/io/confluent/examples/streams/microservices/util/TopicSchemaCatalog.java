package io.confluent.examples.streams.microservices.util;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Demo topic → Avro record field catalog for static field-sanitization analysis.
 */
public final class TopicSchemaCatalog {

  private static final Set<String> ORDER_SENSITIVE_FIELDS =
      Set.of("customerId", "product", "quantity", "price", "state");

  private static final Map<String, Set<String>> TOPIC_VALUE_FIELDS =
      Map.of(
          "orders",
          Set.of("id", "customerId", "state", "product", "quantity", "price"),
          "order-validations",
          Set.of("orderId", "checkType", "validationResult"),
          "warehouse-inventory",
          Set.of("quantity"),
          "orders-enriched",
          Set.of("id", "customerId", "state", "product", "quantity", "price"),
          "payments",
          Set.of("id", "amount"),
          "customers",
          Set.of("id", "name", "phone"));

  private TopicSchemaCatalog() {
  }

  public static Set<String> valueFieldsForTopic(final String topic) {
    if (topic == null) {
      return Set.of();
    }
    return TOPIC_VALUE_FIELDS.getOrDefault(topic, Collections.emptySet());
  }

  public static Set<String> sensitiveOrderFields() {
    return ORDER_SENSITIVE_FIELDS;
  }

  public static boolean isKnownTopic(final String topic) {
    return TOPIC_VALUE_FIELDS.containsKey(topic);
  }

  public static Set<String> copyFields(final Set<String> fields) {
    return new LinkedHashSet<>(fields);
  }
}
