package io.confluent.examples.streams.microservices.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Static analysis helpers over exported processing-policy graphs.
 */
public final class ProcessingPolicyGraphHelper {

  private static final Set<String> AGGREGATION_OPERATORS =
      Set.of(
          "aggregate",
          "reduce",
          "count",
          "join",
          "leftJoin",
          "outerJoin",
          "merge",
          "groupBy",
          "groupByKey",
          "windowedBy");

  private static final Set<String> BRANCH_OPERATORS = Set.of("branch", "split");

  /** Operators that route or combine streams without reducing/transforming record fields. */
  private static final Set<String> PASSTHROUGH_OPERATORS =
      Set.of(
          "join",
          "leftjoin",
          "outerjoin",
          "merge",
          "to",
          "peek",
          "branch",
          "split",
          "selectkey",
          "stream",
          "producer",
          "sendwithtags",
          "tostream",
          "groupby",
          "groupbykey",
          "windowedby");

  private static final Set<String> SANITIZATION_OPERATORS =
      Set.of(
          "filter",
          "map",
          "mapvalues",
          "flatmap",
          "flatmapvalues",
          "selectkey",
          "transform",
          "transformvalues",
          "process",
          "merge",
          "aggregate",
          "reduce",
          "count",
          "join",
          "groupby",
          "groupbykey",
          "windowedby",
          "branch",
          "split",
          "declassifytags",
          "addtags",
          "to");

  private ProcessingPolicyGraphHelper() {
  }

  /**
   * Input topics from which the app reads grantor-tagged data (sources, egress ingress, graph).
   */
  public static Set<String> consumedTopics(
      final AppProcessingPolicy policy,
      final Set<String> grantorTopics) {
    final Set<String> allIngress = new LinkedHashSet<>(policy.getSources());
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      allIngress.addAll(path.getIngressTopics());
      allIngress.addAll(deriveIngressTopicsForEgress(path.getTopic(), policy.getGraph()));
    }
    final Set<String> consumed = new LinkedHashSet<>();
    for (final String topic : allIngress) {
      if (grantorTopics.contains(topic)) {
        consumed.add(topic);
      }
    }
    return consumed;
  }

  public static boolean isProcessingOperator(final String operator) {
    if (operator == null || operator.isEmpty()) {
      return false;
    }
    final String normalized = normalizeOperatorLabel(operator).toLowerCase();
    if (SANITIZATION_OPERATORS.contains(normalized)) {
      return true;
    }
    return normalized.contains("join") || AGGREGATION_OPERATORS.contains(normalized);
  }

  /**
   * Relational / compute sanitization on the data path. {@code addTags}/{@code declassifyTags} alone
   * are label metadata and are not counted as sanitization evidence.
   */
  public static boolean isRelationalSanitizationOperator(final String operator) {
    if (!isProcessingOperator(operator)) {
      return false;
    }
    final String normalized = normalizeOperatorLabel(operator).toLowerCase();
    return !"addtags".equals(normalized) && !"declassifytags".equals(normalized);
  }

  /**
   * Operators that transform record shape or drop fields/records (not join/merge passthrough).
   */
  public static boolean isFieldReducingOperator(final String operator) {
    if (!isRelationalSanitizationOperator(operator)) {
      return false;
    }
    return !PASSTHROUGH_OPERATORS.contains(normalizeOperatorLabel(operator).toLowerCase());
  }

  private enum PathSanitizationRequirement {
    RELATIONAL,
    FIELD_REDUCING
  }

  /**
   * Declared ingress, graph-derived ingress, and app {@code sources} (input topics this service reads).
   */
  public static Set<String> ingressTopicsForEgressPath(
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.EgressPath path) {
    final Set<String> ingress = new LinkedHashSet<>();
    if (path.getIngressTopics() != null) {
      ingress.addAll(path.getIngressTopics());
    }
    if (policy.getGraph() != null) {
      ingress.addAll(deriveIngressTopicsForEgress(path.getTopic(), policy.getGraph()));
    }
    final String egressTopic = path.getTopic();
    for (final String source : policy.getSources()) {
      if (!source.equals(egressTopic) || documentsSameTopicRepublication(policy, path)) {
        ingress.add(source);
      }
    }
    return ingress;
  }

  private static boolean documentsSameTopicRepublication(
      final AppProcessingPolicy policy,
      final AppProcessingPolicy.EgressPath path) {
    if (path == null || path.getTopic() == null || path.getTopic().isEmpty()) {
      return false;
    }
    if (egressPathHasRelationalSanitization(path, policy)) {
      return true;
    }
    return policy.getGraph() != null
        && directedPathHasProcessingOperator(policy.getGraph(), path.getTopic(), path.getTopic());
  }

  public static void enrichEgressPathsFromSources(final AppProcessingPolicy policy) {
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      path.setIngressTopics(new ArrayList<>(ingressTopicsForEgressPath(policy, path)));
    }
  }

  public static boolean egressPathHasRelationalSanitization(
      final AppProcessingPolicy.EgressPath path,
      final AppProcessingPolicy policy) {
    if (path.getOperators().stream().anyMatch(ProcessingPolicyGraphHelper::isRelationalSanitizationOperator)) {
      return true;
    }
    return policy.getAggregations().stream().anyMatch(ProcessingPolicyGraphHelper::isRelationalSanitizationOperator);
  }

  /**
   * Whether the app policy graph documents relational sanitization from {@code sourceTopic} to
   * {@code sinkTopic} (including same-topic republication through an operator loop).
   */
  public static boolean documentsSanitizedPath(
      final AppProcessingPolicy policy,
      final String sourceTopic,
      final String sinkTopic) {
    return documentsSanitizedPath(policy, sourceTopic, sinkTopic, false);
  }

  public static boolean documentsSanitizedPath(
      final AppProcessingPolicy policy,
      final String sourceTopic,
      final String sinkTopic,
      final boolean requireFieldReducing) {
    if (sourceTopic == null || sinkTopic == null || policy.getGraph() == null) {
      return false;
    }
    if (requireFieldReducing) {
      return directedPathHasFieldReducingSanitization(policy.getGraph(), sourceTopic, sinkTopic);
    }
    return directedPathHasRelationalSanitization(policy.getGraph(), sourceTopic, sinkTopic);
  }

  public static boolean directedPathHasFieldReducingSanitization(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    return directedPathHasOperatorMatching(
        graph, sourceTopic, sinkTopic, PathSanitizationRequirement.FIELD_REDUCING);
  }

  public static List<String> findSanitizedNodePathForExtraction(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    return findSanitizedNodePath(
        graph, sourceTopic, sinkTopic, PathSanitizationRequirement.RELATIONAL);
  }

  public static ProcessingPolicyGraph.GraphNode nodeById(
      final ProcessingPolicyGraph graph,
      final String nodeId) {
    return lookupNodeById(graph, nodeId);
  }

  public static ProcessingPolicyGraph.GraphEdge edgeBetween(
      final ProcessingPolicyGraph graph,
      final String fromId,
      final String toId) {
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      if (fromId.equals(edge.getFrom()) && toId.equals(edge.getTo())) {
        return edge;
      }
    }
    return null;
  }

  /**
   * Operator labels along the first graph path from {@code sourceTopic} to {@code sinkTopic} that
   * includes relational sanitization (for RA / field-lineage extraction).
   */
  public static List<String> operatorSequenceOnSanitizedPath(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    final List<String> nodePath =
        findSanitizedNodePath(graph, sourceTopic, sinkTopic, PathSanitizationRequirement.RELATIONAL);
    if (nodePath.isEmpty()) {
      return List.of();
    }
    final List<String> operators = new ArrayList<>();
    for (final String nodeId : nodePath) {
      final ProcessingPolicyGraph.GraphNode node = lookupNodeById(graph, nodeId);
      if (node != null && "operator".equals(node.getKind()) && node.getLabel() != null) {
        operators.add(node.getLabel());
      }
    }
    return operators;
  }

  private static List<String> findSanitizedNodePath(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic,
      final PathSanitizationRequirement requirement) {
    final String sourceId = topicNodeId(graph, sourceTopic);
    final String sinkId = topicNodeId(graph, sinkTopic);
    if (sourceId == null || sinkId == null) {
      return List.of();
    }
    if (sourceId.equals(sinkId)) {
      return findSanitizedLoopPath(graph, sourceId, requirement);
    }
    final Map<String, List<String>> forward = forwardAdjacency(graph);
    final Queue<List<String>> paths = new ArrayDeque<>();
    paths.add(List.of(sourceId));
    final Set<String> visitedPaths = new HashSet<>();
    while (!paths.isEmpty()) {
      final List<String> path = paths.poll();
      final String last = path.get(path.size() - 1);
      final String pathKey = String.join(">", path);
      if (!visitedPaths.add(pathKey)) {
        continue;
      }
      if (sinkId.equals(last)) {
        if (pathSatisfiesRequirement(graph, path, requirement)) {
          return path;
        }
        continue;
      }
      if (path.size() > 64) {
        continue;
      }
      for (final String next : forward.getOrDefault(last, List.of())) {
        if (!path.contains(next)) {
          final List<String> extended = new ArrayList<>(path);
          extended.add(next);
          paths.add(extended);
        }
      }
    }
    return List.of();
  }

  private static List<String> findSanitizedLoopPath(
      final ProcessingPolicyGraph graph,
      final String topicNodeId,
      final PathSanitizationRequirement requirement) {
    final Map<String, List<String>> forward = forwardAdjacency(graph);
    final Queue<List<String>> paths = new ArrayDeque<>();
    paths.add(List.of(topicNodeId));
    final Set<String> visitedPaths = new HashSet<>();
    while (!paths.isEmpty()) {
      final List<String> path = paths.poll();
      final String last = path.get(path.size() - 1);
      final String pathKey = String.join(">", path);
      if (!visitedPaths.add(pathKey)) {
        continue;
      }
      if (topicNodeId.equals(last) && path.size() > 1) {
        if (pathSatisfiesRequirement(graph, path, requirement)) {
          return path;
        }
        continue;
      }
      if (path.size() > 64) {
        continue;
      }
      for (final String next : forward.getOrDefault(last, List.of())) {
        if (!path.contains(next) || topicNodeId.equals(next)) {
          final List<String> extended = new ArrayList<>(path);
          extended.add(next);
          paths.add(extended);
        }
      }
    }
    return List.of();
  }

  /**
   * Whether a graph path from {@code sourceTopic} to {@code sinkTopic} passes through at least one
   * processing operator (relational/static-analysis proxy for sanitization).
   */
  public static boolean directedPathHasRelationalSanitization(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    return directedPathHasOperatorMatching(
        graph, sourceTopic, sinkTopic, PathSanitizationRequirement.RELATIONAL);
  }

  public static boolean directedPathHasProcessingOperator(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    return directedPathHasOperatorMatching(
        graph, sourceTopic, sinkTopic, PathSanitizationRequirement.RELATIONAL);
  }

  private static boolean directedPathHasOperatorMatching(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic,
      final PathSanitizationRequirement requirement) {
    final String sourceId = topicNodeId(graph, sourceTopic);
    final String sinkId = topicNodeId(graph, sinkTopic);
    if (sourceId == null || sinkId == null) {
      return false;
    }
    if (sourceId.equals(sinkId)) {
      return directedLoopHasOperatorMatching(graph, sourceId, requirement);
    }
    final Map<String, List<String>> forward = forwardAdjacency(graph);
    final Queue<List<String>> paths = new ArrayDeque<>();
    paths.add(List.of(sourceId));
    final Set<String> visitedPaths = new HashSet<>();
    while (!paths.isEmpty()) {
      final List<String> path = paths.poll();
      final String last = path.get(path.size() - 1);
      final String pathKey = String.join(">", path);
      if (!visitedPaths.add(pathKey)) {
        continue;
      }
      if (sinkId.equals(last)) {
        if (pathSatisfiesRequirement(graph, path, requirement)) {
          return true;
        }
        continue;
      }
      if (path.size() > 64) {
        continue;
      }
      for (final String next : forward.getOrDefault(last, List.of())) {
        if (!path.contains(next)) {
          final List<String> extended = new ArrayList<>(path);
          extended.add(next);
          paths.add(extended);
        }
      }
    }
    return false;
  }

  /**
   * Same-topic republication: {@code topic → …operators… → topic} with at least one processing step.
   */
  private static boolean directedLoopHasOperatorMatching(
      final ProcessingPolicyGraph graph,
      final String topicNodeId,
      final PathSanitizationRequirement requirement) {
    final Map<String, List<String>> forward = forwardAdjacency(graph);
    final Queue<List<String>> paths = new ArrayDeque<>();
    paths.add(List.of(topicNodeId));
    final Set<String> visitedPaths = new HashSet<>();
    while (!paths.isEmpty()) {
      final List<String> path = paths.poll();
      final String last = path.get(path.size() - 1);
      final String pathKey = String.join(">", path);
      if (!visitedPaths.add(pathKey)) {
        continue;
      }
      if (topicNodeId.equals(last) && path.size() > 1) {
        if (pathSatisfiesRequirement(graph, path, requirement)) {
          return true;
        }
        continue;
      }
      if (path.size() > 64) {
        continue;
      }
      for (final String next : forward.getOrDefault(last, List.of())) {
        if (!path.contains(next) || topicNodeId.equals(next)) {
          final List<String> extended = new ArrayList<>(path);
          extended.add(next);
          paths.add(extended);
        }
      }
    }
    return false;
  }

  private static boolean pathSatisfiesRequirement(
      final ProcessingPolicyGraph graph,
      final List<String> path,
      final PathSanitizationRequirement requirement) {
    boolean hasRelational = false;
    boolean hasFieldReducing = false;
    for (int i = 0; i < path.size(); i++) {
      final String nodeId = path.get(i);
      final ProcessingPolicyGraph.GraphNode node = lookupNodeById(graph, nodeId);
      if (node == null) {
        continue;
      }
      if (i > 0) {
        final ProcessingPolicyGraph.GraphEdge edge =
            edgeBetween(graph, path.get(i - 1), nodeId);
        if (edge != null && isStreamTableProcessingEdge(edge.getLabel())) {
          hasRelational = true;
          if (isFieldReducingStreamTableEdge(edge.getLabel(), node)) {
            hasFieldReducing = true;
          }
        }
      }
      if ("operator".equals(node.getKind())) {
        final String op = normalizeOperatorLabel(node.getLabel());
        if (isRelationalSanitizationOperator(op)) {
          hasRelational = true;
        }
        if (isFieldReducingOperator(op)) {
          hasFieldReducing = true;
        }
      } else if ("stream".equals(node.getKind())
          && node.getLabel() != null
          && node.getLabel().contains("KTable")) {
        hasRelational = true;
        hasFieldReducing = true;
      } else if ("internalTopic".equals(node.getKind())) {
        hasRelational = true;
      } else if ("stateStore".equals(node.getKind())) {
        hasRelational = true;
        hasFieldReducing = true;
      }
    }
    return switch (requirement) {
      case RELATIONAL -> hasRelational;
      case FIELD_REDUCING -> hasFieldReducing;
    };
  }

  private static boolean isStreamTableProcessingEdge(final String label) {
    if (label == null || label.isEmpty()) {
      return false;
    }
    return switch (label) {
      case "repartition-write", "repartition-read", "through-write", "through-read",
          "changelog-write", "changelog-read", "table-source", "table-changelog", "materializes" ->
          true;
      default -> false;
    };
  }

  private static boolean isFieldReducingStreamTableEdge(
      final String label,
      final ProcessingPolicyGraph.GraphNode targetNode) {
    return "table-source".equals(label)
        || "table-changelog".equals(label)
        || "materializes".equals(label)
        || ("internalTopic".equals(targetNode.getKind())
            && "changelog".equals(targetNode.getInternalTopicKind()));
  }

  private static boolean isProcessingGraphNode(final ProcessingPolicyGraph.GraphNode node) {
    if ("operator".equals(node.getKind())) {
      return true;
    }
    if ("stream".equals(node.getKind()) && node.getLabel() != null) {
      return node.getLabel().contains("KTable");
    }
    if ("internalTopic".equals(node.getKind()) || "stateStore".equals(node.getKind())) {
      return true;
    }
    return false;
  }

  /**
   * Topics that feed an egress topic according to the processing graph (backward reachability).
   */
  public static Set<String> deriveIngressTopicsForEgress(
      final String egressTopic,
      final ProcessingPolicyGraph graph) {
    if (egressTopic == null || egressTopic.isEmpty() || graph.getNodes().isEmpty()) {
      return Set.of();
    }
    final String egressNodeId = topicNodeId(graph, egressTopic);
    if (egressNodeId == null) {
      return Set.of();
    }
    final Map<String, List<String>> reverse = reverseAdjacency(graph);
    final Set<String> ingressTopics = new LinkedHashSet<>();
    final Set<String> visited = new HashSet<>();
    final Queue<String> queue = new ArrayDeque<>();
    queue.add(egressNodeId);
    while (!queue.isEmpty()) {
      final String nodeId = queue.poll();
      if (!visited.add(nodeId)) {
        continue;
      }
      final ProcessingPolicyGraph.GraphNode node = lookupNodeById(graph, nodeId);
      if (node != null && "topic".equals(node.getKind()) && !egressTopic.equals(node.getTopic())) {
        ingressTopics.add(node.getTopic());
      }
      for (final String upstream : reverse.getOrDefault(nodeId, List.of())) {
        if (!visited.contains(upstream)) {
          queue.add(upstream);
        }
      }
    }
    return ingressTopics;
  }

  /**
   * Directed reachability from a source topic node to a sink topic node following data-flow edges.
   */
  public static boolean directedTopicReachable(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    final String sourceId = topicNodeId(graph, sourceTopic);
    final String sinkId = topicNodeId(graph, sinkTopic);
    if (sourceId == null || sinkId == null) {
      return false;
    }
    final Map<String, List<String>> forward = forwardAdjacency(graph);
    final Set<String> visited = new HashSet<>();
    final Queue<String> queue = new ArrayDeque<>();
    queue.add(sourceId);
    while (!queue.isEmpty()) {
      final String nodeId = queue.poll();
      if (!visited.add(nodeId)) {
        continue;
      }
      if (sinkId.equals(nodeId)) {
        return true;
      }
      for (final String next : forward.getOrDefault(nodeId, List.of())) {
        if (!visited.contains(next)) {
          queue.add(next);
        }
      }
    }
    return false;
  }

  /**
   * Whether any path from {@code sourceTopic} to {@code sinkTopic} passes through an aggregation operator.
   */
  public static boolean directedPathHasAggregation(
      final ProcessingPolicyGraph graph,
      final String sourceTopic,
      final String sinkTopic) {
    final String sourceId = topicNodeId(graph, sourceTopic);
    final String sinkId = topicNodeId(graph, sinkTopic);
    if (sourceId == null || sinkId == null) {
      return false;
    }
    final Map<String, List<String>> forward = forwardAdjacency(graph);
    final Queue<List<String>> paths = new ArrayDeque<>();
    paths.add(List.of(sourceId));
    final Set<String> visitedPaths = new HashSet<>();
    while (!paths.isEmpty()) {
      final List<String> path = paths.poll();
      final String last = path.get(path.size() - 1);
      final String pathKey = String.join(">", path);
      if (!visitedPaths.add(pathKey)) {
        continue;
      }
      if (sinkId.equals(last)) {
        for (final String nodeId : path) {
          final ProcessingPolicyGraph.GraphNode node = lookupNodeById(graph, nodeId);
          if (node != null && isAggregationNode(node)) {
            return true;
          }
        }
        return false;
      }
      if (path.size() > 64) {
        continue;
      }
      for (final String next : forward.getOrDefault(last, List.of())) {
        if (!path.contains(next)) {
          final List<String> extended = new ArrayList<>(path);
          extended.add(next);
          paths.add(extended);
        }
      }
    }
    return false;
  }

  public static AppProcessingPolicy.AggregationAnalysis computeAggregationAnalysis(
      final AppProcessingPolicy policy) {
    final Map<String, Integer> operatorCounts = new LinkedHashMap<>();
    int joinCount = 0;
    int branchCount = 0;
    int mergeCount = 0;
    int multiSourceEgressPaths = 0;
    int sanitizationOperatorCount = 0;
    int tableSourceCount = 0;

    for (final ProcessingPolicyGraph.GraphNode node : policy.getGraph().getNodes()) {
      if ("stream".equals(node.getKind()) && node.getLabel() != null && node.getLabel().contains("KTable")) {
        tableSourceCount++;
      }
      if (!"operator".equals(node.getKind())) {
        continue;
      }
      final String op = normalizeOperatorLabel(node.getLabel());
      operatorCounts.merge(op, 1, Integer::sum);
      if (isProcessingOperator(op)) {
        sanitizationOperatorCount++;
      }
      if (op.contains("join")) {
        joinCount++;
      } else if (BRANCH_OPERATORS.contains(op)) {
        branchCount++;
      } else if ("merge".equals(op)) {
        mergeCount++;
      }
    }

    for (final String op : policy.getAggregations()) {
      operatorCounts.merge(op, 1, Integer::sum);
    }
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      for (final String op : path.getOperators()) {
        if (AGGREGATION_OPERATORS.contains(op) || BRANCH_OPERATORS.contains(op)) {
          operatorCounts.merge(op, 1, Integer::sum);
        }
      }
      if (path.getIngressTopics().size() > 1) {
        multiSourceEgressPaths++;
      }
    }

    final AppProcessingPolicy.AggregationAnalysis analysis = new AppProcessingPolicy.AggregationAnalysis();
    analysis.setOperatorCounts(operatorCounts);
    analysis.setJoinCount(joinCount);
    analysis.setBranchCount(branchCount);
    analysis.setMergeCount(mergeCount);
    analysis.setMultiSourceEgressPaths(multiSourceEgressPaths);
    analysis.setSanitizationOperatorCount(sanitizationOperatorCount);
    analysis.setTableSourceCount(tableSourceCount);
    analysis.setConsumedGrantorTopics(new ArrayList<>(policy.getSources()));
    analysis.setTotalAggregationOperators(
        operatorCounts.entrySet().stream()
            .filter(e -> AGGREGATION_OPERATORS.contains(e.getKey()) || BRANCH_OPERATORS.contains(e.getKey()))
            .mapToInt(Map.Entry::getValue)
            .sum());
    return analysis;
  }

  public static void enrichEgressPathsFromGraph(final AppProcessingPolicy policy) {
    if (policy.getGraph() == null || policy.getGraph().getNodes().isEmpty()) {
      return;
    }
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      final Set<String> ingress = new LinkedHashSet<>(path.getIngressTopics());
      ingress.addAll(deriveIngressTopicsForEgress(path.getTopic(), policy.getGraph()));
      path.setIngressTopics(new ArrayList<>(ingress));
    }
  }

  public static List<AppProcessingPolicy.EgressPath> egressPathsProcessingTopics(
      final AppProcessingPolicy policy,
      final Set<String> consumedTopics) {
    final List<AppProcessingPolicy.EgressPath> paths = new ArrayList<>();
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      final Set<String> ingress = ingressTopicsForEgressPath(policy, path);
      if (ingress.stream().anyMatch(consumedTopics::contains)) {
        paths.add(path);
      }
    }
    return paths;
  }

  private static boolean isAggregationNode(final ProcessingPolicyGraph.GraphNode node) {
    final String label = node.getLabel() == null ? "" : node.getLabel().toLowerCase();
    for (final String op : AGGREGATION_OPERATORS) {
      if (label.startsWith(op)) {
        return true;
      }
    }
    for (final String op : BRANCH_OPERATORS) {
      if (label.startsWith(op)) {
        return true;
      }
    }
    return "difc".equals(node.getKind());
  }

  private static String normalizeOperatorLabel(final String label) {
    if (label == null || label.isEmpty()) {
      return "operator";
    }
    final int paren = label.indexOf('(');
    return (paren > 0 ? label.substring(0, paren) : label).trim();
  }

  private static ProcessingPolicyGraph.GraphNode lookupNodeById(
      final ProcessingPolicyGraph graph,
      final String id) {
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if (id.equals(node.getId())) {
        return node;
      }
    }
    return null;
  }

  private static String topicNodeId(final ProcessingPolicyGraph graph, final String topic) {
    for (final ProcessingPolicyGraph.GraphNode node : graph.getNodes()) {
      if ("topic".equals(node.getKind()) && topic.equals(node.getTopic())) {
        return node.getId();
      }
    }
    return null;
  }

  private static Map<String, List<String>> forwardAdjacency(final ProcessingPolicyGraph graph) {
    final Map<String, List<String>> adjacency = new HashMap<>();
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      adjacency.computeIfAbsent(edge.getFrom(), k -> new ArrayList<>()).add(edge.getTo());
    }
    return adjacency;
  }

  private static Map<String, List<String>> reverseAdjacency(final ProcessingPolicyGraph graph) {
    final Map<String, List<String>> adjacency = new HashMap<>();
    for (final ProcessingPolicyGraph.GraphEdge edge : graph.getEdges()) {
      adjacency.computeIfAbsent(edge.getTo(), k -> new ArrayList<>()).add(edge.getFrom());
    }
    return adjacency;
  }
}
