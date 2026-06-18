package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Grantor-side verification using expression lineage: detects when egress columns still carry
 * grantor-sensitive information without π sanitization, σ-only filtering, or γ aggregation.
 */
public final class ExpressionLineageVerifier {

  private ExpressionLineageVerifier() {}

  public static boolean scanSanitizesTaggedInput(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String ownedTag,
      final String scanTopic,
      final String sinkTopic) {
    if (path == null || path.getExpressionTree() == null) {
      return false;
    }
    if (RelationalAlgebraTreeVerifier.findScan(path.getExpressionTree(), scanTopic) == null) {
      return false;
    }

    final Set<String> sensitive = GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, scanTopic);
    final Set<String> scanFields = TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(scanTopic));
    final Set<String> taggedSensitive = new LinkedHashSet<>(scanFields);
    taggedSensitive.retainAll(sensitive);
    final Map<String, FieldLineage> egressLineages =
        FieldLineageEvaluator.evaluateEgressLineages(path.getExpressionTree(), sinkTopic);
    if (taggedSensitive.isEmpty()) {
      GrantLineageVerificationLog.expressionLineageDecision(
          ownedTag, scanTopic, sinkTopic, taggedSensitive, egressLineages, Set.of(), true);
      return true;
    }

    if (egressLineages.isEmpty()) {
      GrantLineageVerificationLog.expressionLineageDecision(
          ownedTag, scanTopic, sinkTopic, taggedSensitive, egressLineages, taggedSensitive, false);
      return false;
    }

    if (!scanTopic.equals(sinkTopic) && isSchemaPassthrough(egressLineages, scanFields, path, sinkTopic)) {
      GrantLineageVerificationLog.expressionLineageDecision(
          ownedTag, scanTopic, sinkTopic, taggedSensitive, egressLineages, taggedSensitive, false);
      return false;
    }

    final Set<String> leaked = leakedSensitiveFields(taggedSensitive, egressLineages);
    final boolean sanitized = leaked.isEmpty();
    GrantLineageVerificationLog.expressionLineageDecision(
        ownedTag, scanTopic, sinkTopic, taggedSensitive, egressLineages, leaked, sanitized);
    return sanitized;
  }

  public static Set<String> leakedSensitiveFields(
      final Set<String> taggedSensitive, final Map<String, FieldLineage> egressLineages) {
    final Set<String> leaked = new LinkedHashSet<>();
    if (taggedSensitive == null || taggedSensitive.isEmpty() || egressLineages == null) {
      return leaked;
    }
    for (final String sensitive : taggedSensitive) {
      if (isSensitiveLeaked(sensitive, egressLineages)) {
        leaked.add(sensitive);
      }
    }
    return leaked;
  }

  public static Set<String> retainedSensitiveFields(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String scanTopic,
      final String sinkTopic,
      final Set<String> sensitive) {
    if (path == null || path.getExpressionTree() == null) {
      return Set.of();
    }
    final Map<String, FieldLineage> egress =
        FieldLineageEvaluator.evaluateEgressLineages(path.getExpressionTree(), sinkTopic);
    final Set<String> tagged = new LinkedHashSet<>(sensitive);
    if (scanTopic != null) {
      final Set<String> onScan =
          TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(scanTopic));
      tagged.retainAll(onScan);
    }
    return leakedSensitiveFields(tagged, egress);
  }

  private static boolean isSensitiveLeaked(
      final String sensitive, final Map<String, FieldLineage> egressLineages) {
    for (final FieldLineage lineage : egressLineages.values()) {
      if (lineageLeaksSensitive(sensitive, lineage)) {
        return true;
      }
    }
    return false;
  }

  static boolean lineageLeaksSensitive(final String sensitive, final FieldLineage lineage) {
    if (lineage == null || sensitive == null || sensitive.isEmpty()) {
      return false;
    }
    if (sensitive.equals(lineage.getOutputField())) {
      return !lineage.sanitizesSensitiveSources()
          || lineage.sanitizationKindEnum() == FieldLineage.SanitizationKind.DERIVED;
    }
    if (!lineage.sourceFieldSet().contains(sensitive)) {
      return false;
    }
    return switch (lineage.sanitizationKindEnum()) {
      case BOOLEAN_PREDICATE -> sensitive.equals(lineage.getOutputField());
      case AGGREGATE, CONSTANT, KEY_ONLY, ABSENT -> false;
      case PASSTHROUGH, DERIVED -> true;
    };
  }

  private static boolean isSchemaPassthrough(
      final Map<String, FieldLineage> egressLineages,
      final Set<String> scanFields,
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String sinkTopic) {
    if (egressDocumentsSanitization(path, sinkTopic)) {
      return false;
    }
    final Set<String> egressNames = new LinkedHashSet<>(egressLineages.keySet());
    if (!egressNames.equals(scanFields)) {
      return false;
    }
    for (final FieldLineage lineage : egressLineages.values()) {
      if (lineage.sanitizationKindEnum() != FieldLineage.SanitizationKind.PASSTHROUGH) {
        return false;
      }
      if (!lineage.getOutputField().equals(lineage.sourceFieldSet().stream().findFirst().orElse(""))) {
        return false;
      }
    }
    return true;
  }

  private static boolean egressDocumentsSanitization(
      final AppProcessingPolicy.ProcessingPathAnalysis path, final String sinkTopic) {
    if (path == null) {
      return false;
    }
    if (!path.getSteps().isEmpty()) {
      for (final AppProcessingPolicy.RelationalAlgebraStep step : path.getSteps()) {
        if (step.getOutputFields() != null && !step.getOutputFields().isEmpty()) {
          return true;
        }
      }
    }
    return path.getAlgebraExpression() != null
        && (path.getAlgebraExpression().contains("π[")
            || path.getAlgebraExpression().contains("γ[")
            || path.getAlgebraExpression().contains("mapvalues")
            || path.getAlgebraExpression().contains("process"));
  }
}
