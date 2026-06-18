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
    if (path.getExpressionTree() == null) {
      return false;
    }
    final java.util.Map<String, FieldLineage> egressLineages =
        FieldLineageEvaluator.evaluateEgressLineages(path.getExpressionTree(), outputTopic);
    GrantLineageVerificationLog.verifyingPath(
        ownedTag, inputTopic, outputTopic, egressLineages.size());
    if (!egressLineages.isEmpty()) {
      final boolean lineageSanitized =
          ExpressionLineageVerifier.scanSanitizesTaggedInput(
              path, ownedTag, inputTopic, outputTopic);
      GrantLineageVerificationLog.verificationPath(
          ownedTag, inputTopic, outputTopic, "expression-lineage", lineageSanitized);
      return lineageSanitized;
    }
    if (RelationalAlgebraTreeVerifier.scanSanitizesTaggedInput(
        path, ownedTag, inputTopic, outputTopic)) {
      GrantLineageVerificationLog.verificationPath(
          ownedTag, inputTopic, outputTopic, "relational-algebra-tree", true);
      return true;
    }
    if (agentPathAnalysisSanitizes(path, ownedTag, inputTopic, policy, outputTopic)) {
      GrantLineageVerificationLog.verificationPath(
          ownedTag, inputTopic, outputTopic, "agent-path-analysis", true);
      return true;
    }
    final boolean projectionSanitized =
        PipelineProjectionRegistry.satisfiesDeclassifyViaProjection(
            policy, path, ownedTag, inputTopic, outputTopic);
    GrantLineageVerificationLog.verificationPath(
        ownedTag, inputTopic, outputTopic, "pipeline-projection", projectionSanitized);
    return projectionSanitized;
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
            ? retainedSensitiveForPath(path, inputTopic, outputTopic, sensitive)
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
      if (path.getExpressionTree() != null) {
        if (RelationalAlgebraTreeVerifier.findScan(path.getExpressionTree(), ingressTopic) != null) {
          return path;
        }
        continue;
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

  /**
   * Prefer agent-computed {@code outputFields} / {@code droppedSensitiveFields} only when the
   * signed policy documents real sanitization (operators, declassify, or callbacks) — not sink-schema
   * narrowing alone on a passthrough graph edge.
   */
  static boolean agentPathAnalysisSanitizes(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String ownedTag,
      final String scanTopic,
      final AppProcessingPolicy policy,
      final String egressTopic) {
    if (path == null || path.getOutputFields() == null || path.getOutputFields().isEmpty()) {
      return false;
    }
    if (scanTopic.equals(egressTopic)) {
      return false;
    }
    if (!agentPathHasSanitizationEvidence(policy, path, egressTopic, ownedTag)) {
      return false;
    }
    final Set<String> sensitive = GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, scanTopic);
    if (sensitive.isEmpty()) {
      return true;
    }
    final Set<String> taggedOnScan =
        new LinkedHashSet<>(TopicSchemaCatalog.valueFieldsForTopic(scanTopic));
    taggedOnScan.retainAll(sensitive);
    if (taggedOnScan.isEmpty()) {
      return true;
    }
    final Set<String> output = new LinkedHashSet<>(path.getOutputFields());
    final Set<String> retained = new LinkedHashSet<>(output);
    retained.retainAll(taggedOnScan);
    if (!retained.isEmpty()) {
      return false;
    }
    if (path.getDroppedSensitiveFields().isEmpty()) {
      return false;
    }
    final Set<String> droppedTagged = new LinkedHashSet<>(path.getDroppedSensitiveFields());
    droppedTagged.retainAll(taggedOnScan);
    return droppedTagged.containsAll(taggedOnScan);
  }

  static boolean agentPathHasSanitizationEvidence(
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String egressTopic,
      final String ownedTag) {
    if (policy != null && policy.getEgressPaths() != null) {
      for (final AppProcessingPolicy.EgressPath egress : policy.getEgressPaths()) {
        if (!egressTopic.equals(egress.getTopic())) {
          continue;
        }
        if (egress.getDeclassifyTags() != null && egress.getDeclassifyTags().contains(ownedTag)) {
          return true;
        }
        if (egress.getOperators() != null) {
          for (final String operator : egress.getOperators()) {
            if (ProcessingPolicyGraphHelper.isFieldReducingOperator(operator)) {
              return true;
            }
          }
        }
      }
    }
    if (path != null && path.getAlgebraExpression() != null) {
      final String expr = path.getAlgebraExpression().toLowerCase(java.util.Locale.ROOT);
      if (expr.contains("mapvalues")
          || expr.contains("map(")
          || expr.contains("process")
          || expr.contains("filter")
          || expr.contains("aggregate")
          || expr.contains("reduce")
          || expr.contains("σ[")
          || expr.contains("σ(")
          || expr.contains("π[")) {
        return true;
      }
    }
    return false;
  }

  private static Set<String> retainedSensitiveForPath(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String inputTopic,
      final String outputTopic,
      final Set<String> sensitive) {
    if (path.getExpressionTree() != null) {
      return ExpressionLineageVerifier.retainedSensitiveFields(path, inputTopic, outputTopic, sensitive);
    }
    if (path.getOutputFields() != null && !path.getOutputFields().isEmpty()) {
      final Set<String> fromAgent = retainedFromAgentOutput(path, sensitive);
      if (!fromAgent.isEmpty() || !path.getDroppedSensitiveFields().isEmpty()) {
        return fromAgent;
      }
    }
    return RelationalAlgebraTreeVerifier.retainedSensitiveFields(path, inputTopic, outputTopic, sensitive);
  }

  private static Set<String> retainedFromAgentOutput(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final Set<String> sensitive) {
    if (path == null || path.getOutputFields() == null || path.getOutputFields().isEmpty()) {
      return Set.of();
    }
    final Set<String> retained = new LinkedHashSet<>(path.getOutputFields());
    retained.retainAll(sensitive);
    return retained;
  }
}
