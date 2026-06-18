package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Processing policy exported by the DIFC policy Java agent ({@code processing-policy.json}).
 * Describes how the overall app processes data: ingress topics, operator graph, egress bindings,
 * and DIFC sanitization ({@code declassifyTags}) per egress path.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AppProcessingPolicy {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private int version;
  private String generatedAt;
  private String topologyDigest;
  private String principal;
  private String service;
  private List<String> components;
  private List<String> sources;
  private List<String> aggregations;
  private List<SinkPolicy> sinks;
  private List<EgressPath> egressPaths;
  private ProcessingPolicyGraph graph;
  private AggregationAnalysis aggregationAnalysis;
  private RelationalAlgebraAnalysis relationalAlgebraAnalysis;
  private List<ExternalConnection> externalConnections = new ArrayList<>();

  public AppProcessingPolicy() {
    components = Collections.emptyList();
    sources = Collections.emptyList();
    aggregations = Collections.emptyList();
    sinks = Collections.emptyList();
    egressPaths = Collections.emptyList();
  }

  public int getVersion() {
    return version;
  }

  public String getGeneratedAt() {
    return generatedAt;
  }

  public String getTopologyDigest() {
    return topologyDigest;
  }

  public String getPrincipal() {
    return principal;
  }

  public String getService() {
    return service;
  }

  public List<String> getComponents() {
    return components == null ? Collections.emptyList() : components;
  }

  public List<String> getSources() {
    return sources == null ? Collections.emptyList() : sources;
  }

  public List<String> getAggregations() {
    return aggregations == null ? Collections.emptyList() : aggregations;
  }

  public List<SinkPolicy> getSinks() {
    return sinks == null ? Collections.emptyList() : sinks;
  }

  public List<EgressPath> getEgressPaths() {
    return egressPaths == null ? Collections.emptyList() : egressPaths;
  }

  public ProcessingPolicyGraph getGraph() {
    if (graph == null) {
      graph = new ProcessingPolicyGraph();
    }
    return graph;
  }

  public void setGraph(final ProcessingPolicyGraph graph) {
    this.graph = graph;
  }

  public AggregationAnalysis getAggregationAnalysis() {
    return aggregationAnalysis;
  }

  public void setAggregationAnalysis(final AggregationAnalysis aggregationAnalysis) {
    this.aggregationAnalysis = aggregationAnalysis;
  }

  public void setComponents(final List<String> components) {
    this.components = components;
  }

  public void setSources(final List<String> sources) {
    this.sources = sources;
  }

  public void setAggregations(final List<String> aggregations) {
    this.aggregations = aggregations;
  }

  public void setSinks(final List<SinkPolicy> sinks) {
    this.sinks = sinks;
  }

  public void setEgressPaths(final List<EgressPath> egressPaths) {
    this.egressPaths = egressPaths;
  }

  public boolean hasAggregation() {
    return !getAggregations().isEmpty();
  }

  public static AppProcessingPolicy readFromFile(final Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    if (!Files.isRegularFile(path)) {
      return null;
    }
    return MAPPER.readValue(Files.readString(path), AppProcessingPolicy.class);
  }

  public static AppProcessingPolicy readFromCanonicalJson(final String canonicalJson) throws IOException {
    if (canonicalJson == null || canonicalJson.isEmpty()) {
      return null;
    }
    return MAPPER.readValue(canonicalJson, AppProcessingPolicy.class);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class SinkPolicy {
    private String topic;
    private List<String> declassifyTags;
    private List<String> addTags;

    public SinkPolicy() {
      declassifyTags = Collections.emptyList();
      addTags = Collections.emptyList();
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(final String topic) {
      this.topic = topic;
    }

    public List<String> getDeclassifyTags() {
      return declassifyTags == null ? Collections.emptyList() : declassifyTags;
    }

    public void setDeclassifyTags(final List<String> declassifyTags) {
      this.declassifyTags = declassifyTags;
    }

    public List<String> getAddTags() {
      return addTags == null ? Collections.emptyList() : addTags;
    }

    public void setAddTags(final List<String> addTags) {
      this.addTags = addTags;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class EgressPath {
    private String topic;
    private List<String> ingressTopics;
    private List<String> operators;
    private List<String> declassifyTags;
    private List<String> addTags;

    public EgressPath() {
      ingressTopics = Collections.emptyList();
      operators = Collections.emptyList();
      declassifyTags = Collections.emptyList();
      addTags = Collections.emptyList();
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(final String topic) {
      this.topic = topic;
    }

    public List<String> getIngressTopics() {
      return ingressTopics == null ? Collections.emptyList() : ingressTopics;
    }

    public void setIngressTopics(final List<String> ingressTopics) {
      this.ingressTopics = ingressTopics;
    }

    public List<String> getOperators() {
      return operators == null ? Collections.emptyList() : operators;
    }

    public void setOperators(final List<String> operators) {
      this.operators = operators;
    }

    public List<String> getDeclassifyTags() {
      return declassifyTags == null ? Collections.emptyList() : declassifyTags;
    }

    public void setDeclassifyTags(final List<String> declassifyTags) {
      this.declassifyTags = declassifyTags;
    }

    public List<String> getAddTags() {
      return addTags == null ? Collections.emptyList() : addTags;
    }

    public void setAddTags(final List<String> addTags) {
      this.addTags = addTags;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class AggregationAnalysis {
    private Map<String, Integer> operatorCounts = new LinkedHashMap<>();
    private int joinCount;
    private int branchCount;
    private int mergeCount;
    private int multiSourceEgressPaths;
    private int totalAggregationOperators;
    private int sanitizationOperatorCount;
    private int tableSourceCount;
    private List<String> consumedGrantorTopics;

    public Map<String, Integer> getOperatorCounts() {
      return operatorCounts == null ? Collections.emptyMap() : operatorCounts;
    }

    public void setOperatorCounts(final Map<String, Integer> operatorCounts) {
      this.operatorCounts = operatorCounts == null ? new LinkedHashMap<>() : operatorCounts;
    }

    public int getJoinCount() {
      return joinCount;
    }

    public void setJoinCount(final int joinCount) {
      this.joinCount = joinCount;
    }

    public int getBranchCount() {
      return branchCount;
    }

    public void setBranchCount(final int branchCount) {
      this.branchCount = branchCount;
    }

    public int getMergeCount() {
      return mergeCount;
    }

    public void setMergeCount(final int mergeCount) {
      this.mergeCount = mergeCount;
    }

    public int getMultiSourceEgressPaths() {
      return multiSourceEgressPaths;
    }

    public void setMultiSourceEgressPaths(final int multiSourceEgressPaths) {
      this.multiSourceEgressPaths = multiSourceEgressPaths;
    }

    public int getTotalAggregationOperators() {
      return totalAggregationOperators;
    }

    public void setTotalAggregationOperators(final int totalAggregationOperators) {
      this.totalAggregationOperators = totalAggregationOperators;
    }

    public int getSanitizationOperatorCount() {
      return sanitizationOperatorCount;
    }

    public void setSanitizationOperatorCount(final int sanitizationOperatorCount) {
      this.sanitizationOperatorCount = sanitizationOperatorCount;
    }

    public int getTableSourceCount() {
      return tableSourceCount;
    }

    public void setTableSourceCount(final int tableSourceCount) {
      this.tableSourceCount = tableSourceCount;
    }

    public List<String> getConsumedGrantorTopics() {
      return consumedGrantorTopics == null ? Collections.emptyList() : consumedGrantorTopics;
    }

    public void setConsumedGrantorTopics(final List<String> consumedGrantorTopics) {
      this.consumedGrantorTopics = consumedGrantorTopics;
    }
  }

  public RelationalAlgebraAnalysis getRelationalAlgebraAnalysis() {
    return relationalAlgebraAnalysis;
  }

  public void setRelationalAlgebraAnalysis(final RelationalAlgebraAnalysis relationalAlgebraAnalysis) {
    this.relationalAlgebraAnalysis = relationalAlgebraAnalysis;
  }

  public List<ExternalConnection> getExternalConnections() {
    return externalConnections == null ? Collections.emptyList() : externalConnections;
  }

  public void setExternalConnections(final List<ExternalConnection> externalConnections) {
    this.externalConnections =
        externalConnections == null ? new ArrayList<>() : externalConnections;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class ExternalConnection {
    private String endpoint;
    private boolean allowed;

    public String getEndpoint() {
      return endpoint;
    }

    public void setEndpoint(final String endpoint) {
      this.endpoint = endpoint;
    }

    public boolean isAllowed() {
      return allowed;
    }

    public void setAllowed(final boolean allowed) {
      this.allowed = allowed;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class RelationalAlgebraAnalysis {
    private int tableSourceCount;
    private int repartitionTopicCount;
    private int changelogTopicCount;
    private List<ProcessingPathAnalysis> processingPaths = new ArrayList<>();

    public int getTableSourceCount() {
      return tableSourceCount;
    }

    public void setTableSourceCount(final int tableSourceCount) {
      this.tableSourceCount = tableSourceCount;
    }

    public int getRepartitionTopicCount() {
      return repartitionTopicCount;
    }

    public void setRepartitionTopicCount(final int repartitionTopicCount) {
      this.repartitionTopicCount = repartitionTopicCount;
    }

    public int getChangelogTopicCount() {
      return changelogTopicCount;
    }

    public void setChangelogTopicCount(final int changelogTopicCount) {
      this.changelogTopicCount = changelogTopicCount;
    }

    public List<ProcessingPathAnalysis> getProcessingPaths() {
      return processingPaths == null ? Collections.emptyList() : processingPaths;
    }

    public void setProcessingPaths(final List<ProcessingPathAnalysis> processingPaths) {
      this.processingPaths = processingPaths;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class ProcessingPathAnalysis {
    private String ingressTopic;
    private String egressTopic;
    private String algebraExpression;
    private List<RelationalAlgebraStep> steps = new ArrayList<>();
    private List<String> inputFields = new ArrayList<>();
    private List<String> outputFields = new ArrayList<>();
    private List<String> droppedFields = new ArrayList<>();
    private List<String> retainedFields = new ArrayList<>();
    private List<String> droppedSensitiveFields = new ArrayList<>();
    private double fieldSanitizationRatio;
    private double sensitiveFieldSanitizationRatio;
    private boolean schemaChanged;
    private boolean filtered;
    private boolean aggregated;
    private boolean joined;
    private boolean tableInvolved;
    private boolean repartitionInvolved;
    private boolean changelogInvolved;
    private boolean tableMaterializedFromAggregate;
    private List<String> stateStores = new ArrayList<>();
    private List<String> ingressTopics = new ArrayList<>();
    private RelationalAlgebraExpressionNode expressionTree;

    public List<String> getIngressTopics() {
      return ingressTopics == null ? Collections.emptyList() : ingressTopics;
    }

    public void setIngressTopics(final List<String> ingressTopics) {
      this.ingressTopics = ingressTopics == null ? new ArrayList<>() : ingressTopics;
    }

    public RelationalAlgebraExpressionNode getExpressionTree() {
      return expressionTree;
    }

    public void setExpressionTree(final RelationalAlgebraExpressionNode expressionTree) {
      this.expressionTree = expressionTree;
    }

    public String getIngressTopic() {
      return ingressTopic;
    }

    public void setIngressTopic(final String ingressTopic) {
      this.ingressTopic = ingressTopic;
    }

    public String getEgressTopic() {
      return egressTopic;
    }

    public void setEgressTopic(final String egressTopic) {
      this.egressTopic = egressTopic;
    }

    public String getAlgebraExpression() {
      return algebraExpression;
    }

    public void setAlgebraExpression(final String algebraExpression) {
      this.algebraExpression = algebraExpression;
    }

    public List<RelationalAlgebraStep> getSteps() {
      return steps == null ? Collections.emptyList() : steps;
    }

    public void setSteps(final List<RelationalAlgebraStep> steps) {
      this.steps = steps;
    }

    public List<String> getInputFields() {
      return inputFields == null ? Collections.emptyList() : inputFields;
    }

    public void setInputFields(final List<String> inputFields) {
      this.inputFields = inputFields;
    }

    public List<String> getOutputFields() {
      return outputFields == null ? Collections.emptyList() : outputFields;
    }

    public void setOutputFields(final List<String> outputFields) {
      this.outputFields = outputFields;
    }

    public List<String> getDroppedFields() {
      return droppedFields == null ? Collections.emptyList() : droppedFields;
    }

    public void setDroppedFields(final List<String> droppedFields) {
      this.droppedFields = droppedFields;
    }

    public List<String> getRetainedFields() {
      return retainedFields == null ? Collections.emptyList() : retainedFields;
    }

    public void setRetainedFields(final List<String> retainedFields) {
      this.retainedFields = retainedFields;
    }

    public List<String> getDroppedSensitiveFields() {
      return droppedSensitiveFields == null ? Collections.emptyList() : droppedSensitiveFields;
    }

    public void setDroppedSensitiveFields(final List<String> droppedSensitiveFields) {
      this.droppedSensitiveFields = droppedSensitiveFields;
    }

    public double getFieldSanitizationRatio() {
      return fieldSanitizationRatio;
    }

    public void setFieldSanitizationRatio(final double fieldSanitizationRatio) {
      this.fieldSanitizationRatio = fieldSanitizationRatio;
    }

    public double getSensitiveFieldSanitizationRatio() {
      return sensitiveFieldSanitizationRatio;
    }

    public void setSensitiveFieldSanitizationRatio(final double sensitiveFieldSanitizationRatio) {
      this.sensitiveFieldSanitizationRatio = sensitiveFieldSanitizationRatio;
    }

    public boolean isSchemaChanged() {
      return schemaChanged;
    }

    public void setSchemaChanged(final boolean schemaChanged) {
      this.schemaChanged = schemaChanged;
    }

    public boolean isFiltered() {
      return filtered;
    }

    public void setFiltered(final boolean filtered) {
      this.filtered = filtered;
    }

    public boolean isAggregated() {
      return aggregated;
    }

    public void setAggregated(final boolean aggregated) {
      this.aggregated = aggregated;
    }

    public boolean isJoined() {
      return joined;
    }

    public void setJoined(final boolean joined) {
      this.joined = joined;
    }

    public boolean isTableInvolved() {
      return tableInvolved;
    }

    public void setTableInvolved(final boolean tableInvolved) {
      this.tableInvolved = tableInvolved;
    }

    public boolean isRepartitionInvolved() {
      return repartitionInvolved;
    }

    public void setRepartitionInvolved(final boolean repartitionInvolved) {
      this.repartitionInvolved = repartitionInvolved;
    }

    public boolean isChangelogInvolved() {
      return changelogInvolved;
    }

    public void setChangelogInvolved(final boolean changelogInvolved) {
      this.changelogInvolved = changelogInvolved;
    }

    public boolean isTableMaterializedFromAggregate() {
      return tableMaterializedFromAggregate;
    }

    public void setTableMaterializedFromAggregate(final boolean tableMaterializedFromAggregate) {
      this.tableMaterializedFromAggregate = tableMaterializedFromAggregate;
    }

    public List<String> getStateStores() {
      return stateStores == null ? Collections.emptyList() : stateStores;
    }

    public void setStateStores(final List<String> stateStores) {
      this.stateStores = stateStores;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class RelationalAlgebraStep {
    private String operator;
    private String algebraSymbol;
    private String description;
    private String inputSchema;
    private String outputSchema;
    private List<String> inputFields = new ArrayList<>();
    private List<String> outputFields = new ArrayList<>();
    private String streamTableKind;
    private String internalTopic;
    private String storeName;

    public String getOperator() {
      return operator;
    }

    public void setOperator(final String operator) {
      this.operator = operator;
    }

    public String getAlgebraSymbol() {
      return algebraSymbol;
    }

    public void setAlgebraSymbol(final String algebraSymbol) {
      this.algebraSymbol = algebraSymbol;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(final String description) {
      this.description = description;
    }

    public String getInputSchema() {
      return inputSchema;
    }

    public void setInputSchema(final String inputSchema) {
      this.inputSchema = inputSchema;
    }

    public String getOutputSchema() {
      return outputSchema;
    }

    public void setOutputSchema(final String outputSchema) {
      this.outputSchema = outputSchema;
    }

    public List<String> getInputFields() {
      return inputFields == null ? Collections.emptyList() : inputFields;
    }

    public void setInputFields(final List<String> inputFields) {
      this.inputFields = inputFields;
    }

    public List<String> getOutputFields() {
      return outputFields == null ? Collections.emptyList() : outputFields;
    }

    public void setOutputFields(final List<String> outputFields) {
      this.outputFields = outputFields;
    }

    public String getStreamTableKind() {
      return streamTableKind;
    }

    public void setStreamTableKind(final String streamTableKind) {
      this.streamTableKind = streamTableKind;
    }

    public String getInternalTopic() {
      return internalTopic;
    }

    public void setInternalTopic(final String internalTopic) {
      this.internalTopic = internalTopic;
    }

    public String getStoreName() {
      return storeName;
    }

    public void setStoreName(final String storeName) {
      this.storeName = storeName;
    }
  }

  /** Nested relational-algebra expression (multi-root trees for joins/unions). */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class RelationalAlgebraExpressionNode {
    private String kind;
    private String algebraSymbol;
    private String description;
    private String topic;
    private List<RelationalAlgebraExpressionNode> children = new ArrayList<>();
    private List<String> outputFields = new ArrayList<>();
    private List<String> selectionFields = new ArrayList<>();
    private String selectionExpression = "";
    private List<String> keyFields = new ArrayList<>();
    private List<FieldLineage> fieldLineages = new ArrayList<>();

    public String getKind() {
      return kind;
    }

    public void setKind(final String kind) {
      this.kind = kind;
    }

    public String getAlgebraSymbol() {
      return algebraSymbol;
    }

    public void setAlgebraSymbol(final String algebraSymbol) {
      this.algebraSymbol = algebraSymbol;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(final String description) {
      this.description = description;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(final String topic) {
      this.topic = topic;
    }

    public List<RelationalAlgebraExpressionNode> getChildren() {
      return children == null ? Collections.emptyList() : children;
    }

    public void setChildren(final List<RelationalAlgebraExpressionNode> children) {
      this.children = children == null ? new ArrayList<>() : children;
    }

    public List<String> getOutputFields() {
      return outputFields == null ? Collections.emptyList() : outputFields;
    }

    public void setOutputFields(final List<String> outputFields) {
      this.outputFields = outputFields == null ? new ArrayList<>() : outputFields;
    }

    public List<String> getSelectionFields() {
      return selectionFields == null ? Collections.emptyList() : selectionFields;
    }

    public void setSelectionFields(final List<String> selectionFields) {
      this.selectionFields = selectionFields == null ? new ArrayList<>() : selectionFields;
    }

    public String getSelectionExpression() {
      return selectionExpression == null ? "" : selectionExpression;
    }

    public void setSelectionExpression(final String selectionExpression) {
      this.selectionExpression = selectionExpression == null ? "" : selectionExpression;
    }

    public List<String> getKeyFields() {
      return keyFields == null ? Collections.emptyList() : keyFields;
    }

    public void setKeyFields(final List<String> keyFields) {
      this.keyFields = keyFields == null ? new ArrayList<>() : keyFields;
    }

    public List<FieldLineage> getFieldLineages() {
      return fieldLineages == null ? Collections.emptyList() : fieldLineages;
    }

    public void setFieldLineages(final List<FieldLineage> fieldLineages) {
      this.fieldLineages = fieldLineages == null ? new ArrayList<>() : fieldLineages;
    }
  }
}
