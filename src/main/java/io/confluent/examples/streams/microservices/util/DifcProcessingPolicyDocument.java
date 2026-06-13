package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Machine-readable processing policy emitted by the DIFC policy Java agent
 * ({@code DslGraphTracker.writePolicyJsonFile}) at Streams topology build time.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class DifcProcessingPolicyDocument {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String applicationId = "";
    private String principal = "";
    private String topologyDigest = "";
    private List<String> sourceTopics = List.of();
    private List<String> sinkTopics = List.of();
    private List<String> aggregationOperators = List.of();
    private List<TagLineageEntry> tagLineage = List.of();

    public static DifcProcessingPolicyDocument load(final Path path) throws IOException {
        return MAPPER.readValue(Files.readString(path), DifcProcessingPolicyDocument.class);
    }

    public String applicationId() {
        return applicationId;
    }

    public String principal() {
        return principal;
    }

    public String topologyDigest() {
        return topologyDigest;
    }

    public Set<String> sourceTopics() {
        return new LinkedHashSet<>(sourceTopics);
    }

    public Set<String> sinkTopics() {
        return new LinkedHashSet<>(sinkTopics);
    }

    public Set<String> aggregationOperators() {
        return new LinkedHashSet<>(aggregationOperators);
    }

    public List<TagLineageEntry> tagLineage() {
        return tagLineage;
    }

    public boolean declassifiesTag(final String tag) {
        return tagLineage.stream()
            .anyMatch(entry -> "declassified".equals(entry.role) && tag.equals(entry.tag));
    }

    public boolean producesTag(final String tag) {
        return tagLineage.stream()
            .anyMatch(entry -> "produced".equals(entry.role) && tag.equals(entry.tag));
    }

    public boolean consumesFromTopic(final String topic) {
        if (sourceTopics.contains(topic)) {
            return true;
        }
        return tagLineage.stream()
            .anyMatch(entry -> "consumed".equals(entry.role) && topic.equals(entry.sourceTopic));
    }

    public boolean hasAggregation() {
        return aggregationOperators.stream()
            .anyMatch(op -> Set.of("aggregate", "count", "reduce", "join", "leftJoin", "outerJoin").contains(op));
    }

    public boolean hasJoin() {
        return aggregationOperators.stream()
            .anyMatch(op -> Set.of("join", "leftJoin", "outerJoin").contains(op));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class TagLineageEntry {
        @JsonProperty("tag")
        private String tag = "";
        @JsonProperty("role")
        private String role = "";
        @JsonProperty("sourceTopic")
        private String sourceTopic = "";
        @JsonProperty("sinkTopic")
        private String sinkTopic = "";

        public String tag() {
            return tag;
        }

        public String role() {
            return role;
        }

        public String sourceTopic() {
            return sourceTopic;
        }

        public String sinkTopic() {
            return sinkTopic;
        }
    }

    public void setApplicationId(final String applicationId) {
        this.applicationId = applicationId;
    }

    public void setPrincipal(final String principal) {
        this.principal = principal;
    }

    public void setTopologyDigest(final String topologyDigest) {
        this.topologyDigest = topologyDigest;
    }

    public void setSourceTopics(final List<String> sourceTopics) {
        this.sourceTopics = sourceTopics == null ? List.of() : sourceTopics;
    }

    public void setSinkTopics(final List<String> sinkTopics) {
        this.sinkTopics = sinkTopics == null ? List.of() : sinkTopics;
    }

    public void setAggregationOperators(final List<String> aggregationOperators) {
        this.aggregationOperators = aggregationOperators == null ? List.of() : aggregationOperators;
    }

    public void setTagLineage(final List<TagLineageEntry> tagLineage) {
        this.tagLineage = tagLineage == null ? List.of() : tagLineage;
    }
}
