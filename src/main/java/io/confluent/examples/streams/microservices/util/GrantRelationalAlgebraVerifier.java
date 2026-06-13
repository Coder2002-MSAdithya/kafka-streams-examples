package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Grantor-side checks: every output-topic relation whose input carries tag {@code X} must have a
 * relational-algebra plan (from the processing graph) that sanitizes grantor-defined sensitive fields.
 */
public final class GrantRelationalAlgebraVerifier {

  private GrantRelationalAlgebraVerifier() {
  }

  /**
   * Whether the RA plan for {@code inputTopic → outputTopic} removes all grantor-sensitive fields
   * that appear on the tagged input relation.
   */
  public static boolean relationSanitizesTaggedInput(
      final String ownedTag,
      final String inputTopic,
      final String outputTopic,
      final AppProcessingPolicy policy) {
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        pathAnalysisFor(policy, inputTopic, outputTopic);
    if (path == null) {
      return false;
    }
    if (path.getExpressionTree() != null) {
      return RelationalAlgebraTreeVerifier.scanSanitizesTaggedInput(
          path, ownedTag, inputTopic, outputTopic);
    }
    if (path.getSteps().isEmpty()) {
      return false;
    }
    return sensitiveFieldsSanitized(ownedTag, inputTopic, path);
  }

  public static String denialReason(
      final String ownedTag,
      final String inputTopic,
      final String outputTopic,
      final AppProcessingPolicy.ProcessingPathAnalysis path) {
    if (path == null || path.getAlgebraExpression() == null) {
      return "no relational-algebra plan for "
          + inputTopic
          + " → "
          + outputTopic
          + " (tag "
          + ownedTag
          + ")";
    }
    final Set<String> sensitive = GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, inputTopic);
    final Set<String> retainedSensitive =
        path.getExpressionTree() != null
            ? RelationalAlgebraTreeVerifier.retainedSensitiveFields(path, inputTopic, outputTopic, sensitive)
            : retainedSensitiveFields(path, sensitive);
    return "output relation "
        + inputTopic
        + " → "
        + outputTopic
        + " "
        + path.getAlgebraExpression()
        + " retains grantor-sensitive fields "
        + retainedSensitive
        + " for tag "
        + ownedTag;
  }

  static boolean sensitiveFieldsSanitized(
      final String ownedTag,
      final String inputTopic,
      final AppProcessingPolicy.ProcessingPathAnalysis path) {
    final Set<String> sensitive = GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, inputTopic);
    if (sensitive.isEmpty()) {
      return path.isSchemaChanged() || path.getFieldSanitizationRatio() > 0;
    }

    final Set<String> taggedInputFields = new LinkedHashSet<>(path.getInputFields());
    taggedInputFields.retainAll(sensitive);
    if (taggedInputFields.isEmpty()) {
      return true;
    }

    return retainedSensitiveFields(path, sensitive).isEmpty();
  }

  private static Set<String> retainedSensitiveFields(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final Set<String> sensitive) {
    final Set<String> retained = new LinkedHashSet<>(path.getOutputFields());
    if (retained.isEmpty()) {
      retained.addAll(path.getRetainedFields());
    }
    retained.retainAll(sensitive);
    return retained;
  }

  public static AppProcessingPolicy.ProcessingPathAnalysis pathAnalysisFor(
      final AppProcessingPolicy policy,
      final String ingressTopic,
      final String egressTopic) {
    if (policy == null || policy.getRelationalAlgebraAnalysis() == null) {
      return null;
    }
    for (final AppProcessingPolicy.ProcessingPathAnalysis path :
        policy.getRelationalAlgebraAnalysis().getProcessingPaths()) {
      if (!egressTopic.equals(path.getEgressTopic())) {
        continue;
      }
      if (path.getExpressionTree() != null
          && RelationalAlgebraTreeVerifier.findScan(path.getExpressionTree(), ingressTopic) != null) {
        return path;
      }
      if (ingressTopic.equals(path.getIngressTopic())) {
        return path;
      }
      if (path.getIngressTopics().contains(ingressTopic)) {
        return path;
      }
    }
    return null;
  }
}
