package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Processing graph: topics, operators, and app components wired by data-flow edges.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ProcessingPolicyGraph {

  private List<GraphNode> nodes = new ArrayList<>();
  private List<GraphEdge> edges = new ArrayList<>();

  public List<GraphNode> getNodes() {
    return nodes == null ? Collections.emptyList() : nodes;
  }

  public void setNodes(final List<GraphNode> nodes) {
    this.nodes = nodes == null ? new ArrayList<>() : nodes;
  }

  public List<GraphEdge> getEdges() {
    return edges == null ? Collections.emptyList() : edges;
  }

  public void setEdges(final List<GraphEdge> edges) {
    this.edges = edges == null ? new ArrayList<>() : edges;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class GraphNode {
    private String id;
    private String kind;
    private String label;
    private String topic;
    private String internalTopicKind;
    private String storeName;
    private String tableRole;

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public String getKind() {
      return kind;
    }

    public void setKind(final String kind) {
      this.kind = kind;
    }

    public String getLabel() {
      return label;
    }

    public void setLabel(final String label) {
      this.label = label;
    }

    public String getTopic() {
      return topic;
    }

    public void setTopic(final String topic) {
      this.topic = topic;
    }

    public String getInternalTopicKind() {
      return internalTopicKind;
    }

    public void setInternalTopicKind(final String internalTopicKind) {
      this.internalTopicKind = internalTopicKind;
    }

    public String getStoreName() {
      return storeName;
    }

    public void setStoreName(final String storeName) {
      this.storeName = storeName;
    }

    public String getTableRole() {
      return tableRole;
    }

    public void setTableRole(final String tableRole) {
      this.tableRole = tableRole;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class GraphEdge {
    private String from;
    private String to;
    private String label;

    public String getFrom() {
      return from;
    }

    public void setFrom(final String from) {
      this.from = from;
    }

    public String getTo() {
      return to;
    }

    public void setTo(final String to) {
      this.to = to;
    }

    public String getLabel() {
      return label;
    }

    public void setLabel(final String label) {
      this.label = label;
    }
  }
}
