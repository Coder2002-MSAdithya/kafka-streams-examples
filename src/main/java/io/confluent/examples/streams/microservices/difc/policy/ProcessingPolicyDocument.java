package io.confluent.examples.streams.microservices.difc.policy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Agent-exported (or hand-authored) description of how a Streams app processes tagged data.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessingPolicyDocument {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private int version;
    private String applicationId;
    private String principal;
    private String topologyDigest;
    private List<SinkBinding> sinkBindings = new ArrayList<>();
    private List<TagFlowPolicy> tagFlows = new ArrayList<>();
    private List<String> tableSources = new ArrayList<>();

    public static ProcessingPolicyDocument read(final Path path) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            return null;
        }
        return MAPPER.readValue(Files.readString(path), ProcessingPolicyDocument.class);
    }

    public static void write(final Path path, final ProcessingPolicyDocument document) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), document);
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(final int version) {
        this.version = version;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(final String applicationId) {
        this.applicationId = applicationId;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(final String principal) {
        this.principal = principal;
    }

    public String getTopologyDigest() {
        return topologyDigest;
    }

    public void setTopologyDigest(final String topologyDigest) {
        this.topologyDigest = topologyDigest;
    }

    public List<SinkBinding> getSinkBindings() {
        return sinkBindings;
    }

    public void setSinkBindings(final List<SinkBinding> sinkBindings) {
        this.sinkBindings = sinkBindings == null ? new ArrayList<>() : sinkBindings;
    }

    public List<TagFlowPolicy> getTagFlows() {
        return tagFlows;
    }

    public void setTagFlows(final List<TagFlowPolicy> tagFlows) {
        this.tagFlows = tagFlows == null ? new ArrayList<>() : tagFlows;
    }

    public List<String> getTableSources() {
        return tableSources;
    }

    public void setTableSources(final List<String> tableSources) {
        this.tableSources = tableSources == null ? new ArrayList<>() : tableSources;
    }

    public List<TagFlowPolicy> flowsForTag(final String tag) {
        final List<TagFlowPolicy> matches = new ArrayList<>();
        for (final TagFlowPolicy flow : tagFlows) {
            if (tag != null && tag.equals(flow.getTag())) {
                matches.add(flow);
            }
        }
        if (!matches.isEmpty()) {
            return matches;
        }
        final Set<String> ingress = new LinkedHashSet<>();
        final Set<String> egress = new LinkedHashSet<>();
        final Set<String> aggregators = new LinkedHashSet<>();
        boolean removedOnEgress = false;
        for (final SinkBinding binding : sinkBindings) {
            if (binding.removesTag(tag) || binding.addsTag(tag)) {
                ingress.addAll(binding.getIngressTopics());
                egress.add(binding.getEgressTopic());
                aggregators.addAll(binding.aggregatorOperators());
                if (binding.removesTag(tag)) {
                    removedOnEgress = true;
                }
            }
        }
        for (final SinkBinding binding : sinkBindings) {
            if (binding.getIngressTopics().isEmpty()) {
                continue;
            }
            final boolean consumesTag = binding.removesTag(tag) || binding.addsTag(tag);
            final boolean taggedIngress = binding.getIngressTopics().stream()
                    .anyMatch(topic -> "orders".equals(topic) && "order".equals(tag));
            if (!consumesTag && !taggedIngress) {
                continue;
            }
            final TagFlowPolicy consumeFlow = new TagFlowPolicy();
            consumeFlow.setTag(tag);
            consumeFlow.setIngressTopics(new ArrayList<>(binding.getIngressTopics()));
            if (binding.getEgressTopic() != null) {
                consumeFlow.setEgressTopics(List.of(binding.getEgressTopic()));
            }
            consumeFlow.setRemovedOnEgress(binding.removesTag(tag));
            consumeFlow.setAggregators(new ArrayList<>(binding.aggregatorOperators()));
            matches.add(consumeFlow);
        }
        if (!matches.isEmpty()) {
            return matches;
        }
        if (ingress.isEmpty() && egress.isEmpty()) {
            return List.of();
        }
        final TagFlowPolicy derived = new TagFlowPolicy();
        derived.setTag(tag);
        derived.setIngressTopics(new ArrayList<>(ingress));
        derived.setEgressTopics(new ArrayList<>(egress));
        derived.setRemovedOnEgress(removedOnEgress);
        derived.setAggregators(new ArrayList<>(aggregators));
        return List.of(derived);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SinkBinding {
        private String egressTopic;
        private List<String> ingressTopics = new ArrayList<>();
        private List<String> addedTags = new ArrayList<>();
        private List<String> removedTags = new ArrayList<>();
        private List<String> operators = new ArrayList<>();

        public String getEgressTopic() {
            return egressTopic;
        }

        public void setEgressTopic(final String egressTopic) {
            this.egressTopic = egressTopic;
        }

        public List<String> getIngressTopics() {
            return ingressTopics;
        }

        public void setIngressTopics(final List<String> ingressTopics) {
            this.ingressTopics = ingressTopics == null ? new ArrayList<>() : ingressTopics;
        }

        public List<String> getAddedTags() {
            return addedTags;
        }

        public void setAddedTags(final List<String> addedTags) {
            this.addedTags = addedTags == null ? new ArrayList<>() : addedTags;
        }

        public List<String> getRemovedTags() {
            return removedTags;
        }

        public void setRemovedTags(final List<String> removedTags) {
            this.removedTags = removedTags == null ? new ArrayList<>() : removedTags;
        }

        public List<String> getOperators() {
            return operators;
        }

        public void setOperators(final List<String> operators) {
            this.operators = operators == null ? new ArrayList<>() : operators;
        }

        public boolean addsTag(final String tag) {
            return addedTags.contains(tag);
        }

        public boolean removesTag(final String tag) {
            return removedTags.contains(tag);
        }

        public Set<String> aggregatorOperators() {
            final Set<String> aggregators = new LinkedHashSet<>();
            for (final String op : operators) {
                if (TagFlowPolicy.isAggregatorOperator(op)) {
                    aggregators.add(op);
                }
            }
            return aggregators;
        }
    }
}
