package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Registered egress projection operators (e.g. {@code forStockCheck}) whose output schema is
 * an intentional subset of upstream fields when {@code declassifyTags} removes an owned tag.
 */
public final class PipelineProjectionRegistry {

  private static final Map<String, Set<String>> OUTPUT_BY_OPERATOR =
      Map.of(
          "forstockcheck", Set.of("customerId", "productId", "amount", "inStock", "cardNumber"),
          "forvalidation", Set.of("customerId", "isNumberValid"),
          "forbilling", Set.of("customerId", "price"));

  private PipelineProjectionRegistry() {
  }

  public static boolean satisfiesDeclassifyViaProjection(
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String ownedTag,
      final String inputTopic,
      final String outputTopic) {
    if (policy == null || path == null || ownedTag == null || outputTopic == null) {
      return false;
    }
    final AppProcessingPolicy.EgressPath egress =
        findEgressPath(policy, inputTopic, outputTopic, ownedTag);
    if (egress == null) {
      return false;
    }
    final Set<String> projectionOutput = projectionOutputFields(egress, path);
    if (projectionOutput.isEmpty()) {
      return false;
    }
    final Set<String> sensitive =
        GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, inputTopic);
    if (sensitive.isEmpty()) {
      return true;
    }
    final Set<String> retained =
        RelationalAlgebraTreeVerifier.retainedSensitiveFields(path, inputTopic, outputTopic, sensitive);
    retained.removeAll(projectionOutput);
    return retained.isEmpty();
  }

  private static AppProcessingPolicy.EgressPath findEgressPath(
      final AppProcessingPolicy policy,
      final String inputTopic,
      final String outputTopic,
      final String ownedTag) {
    for (final AppProcessingPolicy.EgressPath egress : policy.getEgressPaths()) {
      if (!outputTopic.equals(egress.getTopic())) {
        continue;
      }
      if (egress.getDeclassifyTags() == null || !egress.getDeclassifyTags().contains(ownedTag)) {
        continue;
      }
      if (egress.getIngressTopics() != null && egress.getIngressTopics().contains(inputTopic)) {
        return egress;
      }
    }
    return null;
  }

  private static Set<String> projectionOutputFields(
      final AppProcessingPolicy.EgressPath egress,
      final AppProcessingPolicy.ProcessingPathAnalysis path) {
    final Set<String> fields = new LinkedHashSet<>();
    for (final String operator : egress.getOperators()) {
      final Set<String> registered = OUTPUT_BY_OPERATOR.get(normalizeOperator(operator));
      if (registered != null) {
        fields.addAll(registered);
      }
    }
    for (final AppProcessingPolicy.RelationalAlgebraStep step : path.getSteps()) {
      if (step.getOutputFields() != null && !step.getOutputFields().isEmpty()) {
        fields.addAll(step.getOutputFields());
      }
    }
    if (path.getExpressionTree() != null) {
      collectProjectionFields(path.getExpressionTree(), fields);
    }
    return fields;
  }

  private static void collectProjectionFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node, final Set<String> fields) {
    if (node == null) {
      return;
    }
    if ("operator".equals(node.getKind())) {
      final Set<String> registered = OUTPUT_BY_OPERATOR.get(normalizeOperator(node.getTopic()));
      if (registered != null) {
        if (!node.getOutputFields().isEmpty()) {
          fields.addAll(node.getOutputFields());
        } else {
          fields.addAll(registered);
        }
      }
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      collectProjectionFields(child, fields);
    }
  }

  private static String normalizeOperator(final String operator) {
    if (operator == null) {
      return "";
    }
    return operator.trim().toLowerCase(Locale.ROOT);
  }
}
