package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.Set;

/** Grantor-side evaluation of tree-structured relational-algebra expressions. */
public final class RelationalAlgebraTreeVerifier {

  private RelationalAlgebraTreeVerifier() {
  }

  public static boolean scanSanitizesTaggedInput(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String ownedTag,
      final String scanTopic,
      final String sinkTopic) {
    if (path == null || path.getExpressionTree() == null) {
      return false;
    }
    final AppProcessingPolicy.RelationalAlgebraExpressionNode scan =
        findScan(path.getExpressionTree(), scanTopic);
    if (scan == null) {
      return false;
    }

    final Set<String> sensitive = GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, scanTopic);
    final Set<String> scanFields = TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(scanTopic));
    final Set<String> taggedSensitive = new LinkedHashSet<>(scanFields);
    taggedSensitive.retainAll(sensitive);
    if (taggedSensitive.isEmpty()) {
      return true;
    }

    final Set<String> finalFields =
        resolveEgressOutputFields(path.getExpressionTree(), sinkTopic, path);
    if (!egressDocumentsSanitization(path, sinkTopic)
        && !scanTopic.equals(sinkTopic)
        && finalFields.equals(scanFields)) {
      return false;
    }
    final Set<String> retainedSensitive = new LinkedHashSet<>(finalFields);
    retainedSensitive.retainAll(taggedSensitive);
    return retainedSensitive.isEmpty();
  }

  public static Set<String> retainedSensitiveFields(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String scanTopic,
      final String sinkTopic,
      final Set<String> sensitive) {
    if (path == null || path.getExpressionTree() == null) {
      return Set.of();
    }
    final Set<String> finalFields =
        resolveEgressOutputFields(path.getExpressionTree(), sinkTopic, path);
    final Set<String> retained = new LinkedHashSet<>(finalFields);
    retained.retainAll(sensitive);
    return retained;
  }

  public static AppProcessingPolicy.ProcessingPathAnalysis pathForEgress(
      final AppProcessingPolicy policy,
      final String egressTopic) {
    if (policy == null || policy.getRelationalAlgebraAnalysis() == null) {
      return null;
    }
    for (final AppProcessingPolicy.ProcessingPathAnalysis path :
        policy.getRelationalAlgebraAnalysis().getProcessingPaths()) {
      if (egressTopic.equals(path.getEgressTopic())) {
        return path;
      }
    }
    return null;
  }

  static AppProcessingPolicy.RelationalAlgebraExpressionNode findScan(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String topic) {
    if (node == null) {
      return null;
    }
    if ("scan".equals(node.getKind()) && topic.equals(node.getTopic())) {
      return node;
    }
    for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
      final AppProcessingPolicy.RelationalAlgebraExpressionNode found = findScan(child, topic);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  static Set<String> evaluateOutputFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    if (node == null) {
      return Set.of();
    }
    if ("sink".equals(node.getKind())) {
      return evaluateChildOutput(node, sinkTopic);
    }
    if ("operator".equals(node.getKind()) && shouldRecomputeOperatorFields(node)) {
      return evaluateOperatorOutput(node, sinkTopic);
    }
    if (!node.getOutputFields().isEmpty()) {
      return new LinkedHashSet<>(node.getOutputFields());
    }
    return switch (node.getKind()) {
      case "scan" -> TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(node.getTopic()));
      case "operator" -> evaluateOperatorOutput(node, sinkTopic);
      default -> Set.of();
    };
  }

  private static boolean shouldRecomputeOperatorFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node) {
    final String op = node.getTopic() == null ? "" : node.getTopic();
    return Set.of("join", "leftjoin", "outerjoin", "merge", "aggregate", "reduce", "count")
        .contains(op);
  }

  static Set<String> resolveEgressOutputFields(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode root,
      final String sinkTopic,
      final AppProcessingPolicy.ProcessingPathAnalysis path) {
    final Set<String> pipelineFields = evaluateOutputFields(root, sinkTopic);
    if (!egressDocumentsSanitization(path, sinkTopic)) {
      return pipelineFields;
    }
    if (schemaNarrowingEgress(sinkTopic, pipelineFields)) {
      return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(sinkTopic));
    }
    return pipelineFields;
  }

  private static boolean egressDocumentsSanitization(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String sinkTopic) {
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
        && containsSanitizingOperator(path.getAlgebraExpression());
  }

  private static boolean containsSanitizingOperator(final String algebraExpression) {
    final String expr = algebraExpression.toLowerCase(java.util.Locale.ROOT);
    return expr.contains("mapvalues")
        || expr.contains("map(")
        || expr.contains("process")
        || expr.contains("filter")
        || expr.contains("aggregate")
        || expr.contains("reduce")
        || expr.contains("σ[")
        || expr.contains("σ(");
  }

  private static Set<String> evaluateChildOutput(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    if (node.getChildren().isEmpty()) {
      return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(sinkTopic));
    }
    return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
  }

  /** True when a known sink schema drops order-sensitive fields present in the pipeline output. */
  private static boolean schemaNarrowingEgress(
      final String sinkTopic,
      final Set<String> pipelineFields) {
    if (!TopicSchemaCatalog.isKnownTopic(sinkTopic) || pipelineFields.isEmpty()) {
      return false;
    }
    final Set<String> sinkSchema = TopicSchemaCatalog.valueFieldsForTopic(sinkTopic);
    if (sinkSchema.isEmpty() || sinkSchema.equals(pipelineFields)) {
      return false;
    }
    final Set<String> retainedOrderSensitive = new LinkedHashSet<>(pipelineFields);
    retainedOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    final Set<String> sinkOrderSensitive = new LinkedHashSet<>(sinkSchema);
    sinkOrderSensitive.retainAll(TopicSchemaCatalog.sensitiveOrderFields());
    return !retainedOrderSensitive.isEmpty() && sinkOrderSensitive.isEmpty();
  }

  private static Set<String> evaluateOperatorOutput(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    final String op = node.getTopic() == null ? "" : node.getTopic();
    if (Set.of("join", "leftjoin", "outerjoin").contains(op)) {
      if ("orders".equals(sinkTopic)) {
        return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic("orders"));
      }
      final Set<String> merged = new LinkedHashSet<>();
      for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
        merged.addAll(evaluateOutputFields(child, sinkTopic));
      }
      return merged;
    }
    if ("merge".equals(op)) {
      final Set<String> merged = new LinkedHashSet<>();
      for (final AppProcessingPolicy.RelationalAlgebraExpressionNode child : node.getChildren()) {
        merged.addAll(evaluateOutputFields(child, sinkTopic));
      }
      return merged;
    }
    if (Set.of("mapvalues", "map", "flatmapvalues", "flatmap", "transform", "transformvalues", "process")
        .contains(op)) {
      if (!node.getOutputFields().isEmpty()) {
        return new LinkedHashSet<>(node.getOutputFields());
      }
      if (node.getChildren().isEmpty()) {
        return Set.of();
      }
      return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
    }
    if (Set.of("aggregate", "reduce", "count").contains(op)) {
      if ("orders".equals(sinkTopic)) {
        return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic("orders"));
      }
      if (!node.getOutputFields().isEmpty()) {
        return new LinkedHashSet<>(node.getOutputFields());
      }
      if (node.getChildren().isEmpty()) {
        return Set.of("_aggregate_value");
      }
      return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
    }
    if (node.getChildren().isEmpty()) {
      return Set.of();
    }
    return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
  }
}
