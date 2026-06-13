package io.confluent.examples.streams.microservices.difc.policy;

import java.util.List;

/**
 * Hand-authored policies for apps that do not build their topology purely via instrumented Streams DSL
 * (e.g. REST + {@code KafkaProducer} in OrdersService).
 */
public final class DifcPolicyManifests {

    private DifcPolicyManifests() {
    }

    public static ProcessingPolicyDocument ordersProducer() {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId("OrdersService");
        doc.setPrincipal("orders-svc");

        final ProcessingPolicyDocument.SinkBinding ordersEgress = new ProcessingPolicyDocument.SinkBinding();
        ordersEgress.setEgressTopic("orders");
        ordersEgress.setIngressTopics(List.of());
        ordersEgress.setAddedTags(List.of("order"));
        ordersEgress.setRemovedTags(List.of());
        ordersEgress.setOperators(List.of("producer", "sendWithTags"));
        doc.setSinkBindings(List.of(ordersEgress));

        final TagFlowPolicy orderFlow = new TagFlowPolicy();
        orderFlow.setTag("order");
        orderFlow.setIngressTopics(List.of());
        orderFlow.setEgressTopics(List.of("orders"));
        orderFlow.setRemovedOnEgress(false);
        orderFlow.setAggregators(List.of());
        doc.setTagFlows(List.of(orderFlow));
        return doc;
    }

    public static ProcessingPolicyDocument emailConsumer() {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId("EmailService");
        doc.setPrincipal("email-svc");

        final ProcessingPolicyDocument.SinkBinding enriched = new ProcessingPolicyDocument.SinkBinding();
        enriched.setEgressTopic("orders-enriched");
        enriched.setIngressTopics(List.of("orders", "payments", "customers"));
        enriched.setOperators(List.of("stream", "join"));
        doc.setSinkBindings(List.of(enriched));

        final TagFlowPolicy orderFlow = new TagFlowPolicy();
        orderFlow.setTag("order");
        orderFlow.setIngressTopics(List.of("orders"));
        orderFlow.setEgressTopics(List.of("orders-enriched"));
        orderFlow.setRemovedOnEgress(false);
        orderFlow.setAggregators(List.of("join"));
        doc.setTagFlows(List.of(orderFlow));
        return doc;
    }

    public static ProcessingPolicyDocument fraudValidator() {
        return validatorSink(
            "FraudService",
            "fraud-svc",
            "fraud",
            List.of("filter", "groupBy", "windowedBy", "aggregate", "split", "mapValues", "merge"));
    }

    public static ProcessingPolicyDocument inventoryValidator() {
        final ProcessingPolicyDocument doc = validatorSink(
            "InventoryService",
            "inventory-svc",
            "inv-valid",
            List.of("selectKey", "filter", "join", "mapValues", "process"));
        doc.setTableSources(List.of("warehouse-inventory"));
        return doc;
    }

    public static ProcessingPolicyDocument orderDetailsValidator() {
        return validatorSink(
            "OrderDetailsService",
            "order-details-svc",
            "order-valid",
            List.of("filter", "mapValues"));
    }

    private static ProcessingPolicyDocument validatorSink(
            final String applicationId,
            final String principal,
            final String validationTag,
            final List<String> operators) {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId(applicationId);
        doc.setPrincipal(principal);

        final ProcessingPolicyDocument.SinkBinding validationsOut = new ProcessingPolicyDocument.SinkBinding();
        validationsOut.setEgressTopic("order-validations");
        validationsOut.setIngressTopics(List.of("orders"));
        validationsOut.setRemovedTags(List.of("order"));
        validationsOut.setAddedTags(List.of(validationTag));
        validationsOut.setOperators(operators);
        doc.setSinkBindings(List.of(validationsOut));
        return doc;
    }

    public static ProcessingPolicyDocument validationsAggregator() {
        final ProcessingPolicyDocument doc = new ProcessingPolicyDocument();
        doc.setVersion(1);
        doc.setApplicationId("ValidationsAggregatorService");
        doc.setPrincipal("validations-agg-svc");

        final ProcessingPolicyDocument.SinkBinding ordersOut = new ProcessingPolicyDocument.SinkBinding();
        ordersOut.setEgressTopic("orders");
        ordersOut.setIngressTopics(List.of("order-validations", "orders"));
        ordersOut.setRemovedTags(List.of("fraud", "inv-valid", "order-valid", "order"));
        ordersOut.setOperators(List.of("stream", "aggregate", "join", "merge", "declassifyTags", "to"));
        doc.setSinkBindings(List.of(ordersOut));

        final TagFlowPolicy orderFlow = new TagFlowPolicy();
        orderFlow.setTag("order");
        orderFlow.setIngressTopics(List.of("orders"));
        orderFlow.setEgressTopics(List.of("orders"));
        orderFlow.setRemovedOnEgress(true);
        orderFlow.setAggregators(List.of("aggregate", "join", "merge"));
        doc.setTagFlows(List.of(orderFlow));
        return doc;
    }
}
