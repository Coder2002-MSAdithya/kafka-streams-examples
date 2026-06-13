package io.confluent.examples.streams.microservices.util;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Maps grantor-owned DIFC tags to the Kafka topics on which that grantor attaches the tag to
 * published message labels. Used at grant time by the tag owner (grantor).
 */
public final class GrantorTagTopology {

  private GrantorTagTopology() {
  }

  /**
   * Topics on which {@code tagName} appears in the label set of messages published in this demo.
   */
  public static List<String> outputTopicsForTag(final String tagName) {
    if (tagName == null || tagName.isEmpty()) {
      return Collections.emptyList();
    }
    return switch (tagName) {
      case DifcGrantPolicy.TAG_ORDER -> List.of("orders");
      case DifcGrantPolicy.TAG_FRAUD,
           DifcGrantPolicy.TAG_INV_VALID,
           DifcGrantPolicy.TAG_ORDER_VALID -> List.of("order-validations");
      default -> Collections.emptyList();
    };
  }

  /**
   * Topics the grantor publishes to with {@code tagName} on the label (empty if grantor does not own tag).
   */
  public static List<String> grantorPublishTopicsForTag(
      final String grantorPrincipal,
      final String tagName) {
    if (!ownsTag(grantorPrincipal, tagName)) {
      return Collections.emptyList();
    }
    return outputTopicsForTag(tagName);
  }

  public static boolean ownsTag(final String grantorPrincipal, final String tagName) {
    if (grantorPrincipal == null || tagName == null) {
      return false;
    }
    return switch (tagName) {
      case DifcGrantPolicy.TAG_ORDER -> DifcGrantPolicy.PRINCIPAL_ORDERS.equals(grantorPrincipal);
      case DifcGrantPolicy.TAG_FRAUD -> DifcGrantPolicy.PRINCIPAL_FRAUD.equals(grantorPrincipal);
      case DifcGrantPolicy.TAG_INV_VALID -> DifcGrantPolicy.PRINCIPAL_INVENTORY.equals(grantorPrincipal);
      case DifcGrantPolicy.TAG_ORDER_VALID ->
          DifcGrantPolicy.PRINCIPAL_ORDER_DETAILS.equals(grantorPrincipal);
      default -> false;
    };
  }

  /** Fields the grantor treats as sensitive for tagged data read from {@code inputTopic}. */
  public static java.util.Set<String> sensitiveFieldsForTaggedInput(
      final String tagName,
      final String inputTopic) {
    if (tagName == null || inputTopic == null || inputTopic.isEmpty()) {
      return java.util.Set.of();
    }
    if (!outputTopicsForTag(tagName).contains(inputTopic)) {
      return java.util.Set.of();
    }
    if (DifcGrantPolicy.TAG_ORDER.equals(tagName)) {
      return TopicSchemaCatalog.sensitiveOrderFields();
    }
    return TopicSchemaCatalog.valueFieldsForTopic(inputTopic);
  }

  /** Running service application id → authenticated SCRAM principal. */
  public static String principalForServiceName(final String serviceName) {
    Objects.requireNonNull(serviceName, "serviceName");
    return switch (serviceName) {
      case "OrdersService" -> DifcGrantPolicy.PRINCIPAL_ORDERS;
      case "FraudService" -> DifcGrantPolicy.PRINCIPAL_FRAUD;
      case "InventoryService" -> DifcGrantPolicy.PRINCIPAL_INVENTORY;
      case "OrderDetailsService" -> DifcGrantPolicy.PRINCIPAL_ORDER_DETAILS;
      case "EmailService" -> DifcGrantPolicy.PRINCIPAL_EMAIL;
      case "ValidationsAggregatorService" -> DifcGrantPolicy.PRINCIPAL_VALIDATIONS_AGG;
      default -> serviceName;
    };
  }
}
