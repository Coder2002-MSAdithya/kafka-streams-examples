package io.confluent.examples.streams.microservices.difc.policy;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TagFlowPolicy {

    private String tag;
    private List<String> ingressTopics = new ArrayList<>();
    private List<String> egressTopics = new ArrayList<>();
    private boolean removedOnEgress;
    private List<String> aggregators = new ArrayList<>();

    public String getTag() {
        return tag;
    }

    public void setTag(final String tag) {
        this.tag = tag;
    }

    public List<String> getIngressTopics() {
        return ingressTopics;
    }

    public void setIngressTopics(final List<String> ingressTopics) {
        this.ingressTopics = ingressTopics == null ? new ArrayList<>() : ingressTopics;
    }

    public List<String> getEgressTopics() {
        return egressTopics;
    }

    public void setEgressTopics(final List<String> egressTopics) {
        this.egressTopics = egressTopics == null ? new ArrayList<>() : egressTopics;
    }

    public boolean isRemovedOnEgress() {
        return removedOnEgress;
    }

    public void setRemovedOnEgress(final boolean removedOnEgress) {
        this.removedOnEgress = removedOnEgress;
    }

    public List<String> getAggregators() {
        return aggregators;
    }

    public void setAggregators(final List<String> aggregators) {
        this.aggregators = aggregators == null ? new ArrayList<>() : aggregators;
    }

    public boolean ingestsFromAny(final Set<String> grantorEgressTopics) {
        for (final String ingress : ingressTopics) {
            if (grantorEgressTopics.contains(ingress)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasAggregator() {
        return !aggregators.isEmpty();
    }

    public boolean hasJoinAggregator() {
        for (final String op : aggregators) {
            if (op.contains("join") || "join".equals(op) || "leftJoin".equals(op) || "outerJoin".equals(op)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAggregatorOperator(final String operator) {
        return "aggregate".equals(operator)
                || "reduce".equals(operator)
                || "merge".equals(operator)
                || "join".equals(operator)
                || "leftJoin".equals(operator)
                || "outerJoin".equals(operator)
                || "count".equals(operator);
    }
}
