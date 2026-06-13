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

    final Set<String> finalFields = evaluateOutputFields(path.getExpressionTree(), sinkTopic);
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
    final Set<String> finalFields = evaluateOutputFields(path.getExpressionTree(), sinkTopic);
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
    if (!node.getOutputFields().isEmpty()) {
      return new LinkedHashSet<>(node.getOutputFields());
    }
    return switch (node.getKind()) {
      case "scan" -> TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(node.getTopic()));
      case "sink" -> evaluateChildOutput(node, sinkTopic);
      case "operator" -> evaluateOperatorOutput(node, sinkTopic);
      default -> Set.of();
    };
  }

  private static Set<String> evaluateChildOutput(
      final AppProcessingPolicy.RelationalAlgebraExpressionNode node,
      final String sinkTopic) {
    if (node.getChildren().isEmpty()) {
      return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic(sinkTopic));
    }
    return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
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
      if ("order-validations".equals(sinkTopic)) {
        return TopicSchemaCatalog.copyFields(TopicSchemaCatalog.valueFieldsForTopic("order-validations"));
      }
    }
    if (Set.of("aggregate", "reduce", "count").contains(op)) {
      return Set.of("_aggregate_value");
    }
    if (node.getChildren().isEmpty()) {
      return Set.of();
    }
    return evaluateOutputFields(node.getChildren().get(0), sinkTopic);
  }
}
