package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpressionLineageVerifierTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void booleanProjectionSanitizesSensitivePrice() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "egressTopic": "order-validations",
              "expressionTree": {
                "kind": "sink",
                "topic": "order-validations",
                "children": [{
                  "kind": "operator",
                  "topic": "mapvalues",
                  "outputFields": ["orderId", "isHighValue"],
                  "fieldLineages": [
                    {
                      "outputField": "orderId",
                      "valueType": "STRING",
                      "sourceFields": ["id"],
                      "expression": "id",
                      "sanitizationKind": "PASSTHROUGH"
                    },
                    {
                      "outputField": "isHighValue",
                      "valueType": "BOOLEAN",
                      "sourceFields": ["price"],
                      "expression": "price ? bool",
                      "sanitizationKind": "BOOLEAN_PREDICATE"
                    }
                  ],
                  "children": [{"kind": "scan", "topic": "orders"}]
                }]
              }
            }]
          }
        }
        """);
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        policy.getRelationalAlgebraAnalysis().getProcessingPaths().get(0);
    assertTrue(
        ExpressionLineageVerifier.scanSanitizesTaggedInput(
            path, DifcGrantPolicy.TAG_ORDER, "orders", "order-validations"));
  }

  @Test
  void filterDoesNotSanitizeSensitiveColumns() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "egressTopic": "order-validations",
              "expressionTree": {
                "kind": "sink",
                "topic": "order-validations",
                "children": [{
                  "kind": "operator",
                  "topic": "filter",
                  "selectionExpression": "price",
                  "children": [{
                    "kind": "operator",
                    "topic": "mapvalues",
                    "outputFields": ["orderId", "price", "customerId"],
                    "children": [{"kind": "scan", "topic": "orders"}]
                  }]
                }]
              }
            }]
          }
        }
        """);
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        policy.getRelationalAlgebraAnalysis().getProcessingPaths().get(0);
    assertFalse(
        ExpressionLineageVerifier.scanSanitizesTaggedInput(
            path, DifcGrantPolicy.TAG_ORDER, "orders", "order-validations"));
  }

  @Test
  void aggregateOnlyOutputSanitizesSensitiveScalars() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "egressTopic": "order-validations",
              "expressionTree": {
                "kind": "sink",
                "topic": "order-validations",
                "children": [{
                  "kind": "operator",
                  "topic": "aggregate",
                  "fieldLineages": [{
                    "outputField": "_aggregate_value",
                    "valueType": "AGGREGATE",
                    "sourceFields": ["price", "quantity"],
                    "expression": "γ(price, quantity)",
                    "sanitizationKind": "AGGREGATE"
                  }],
                  "children": [{"kind": "scan", "topic": "orders"}]
                }]
              }
            }]
          }
        }
        """);
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        policy.getRelationalAlgebraAnalysis().getProcessingPaths().get(0);
    assertTrue(
        ExpressionLineageVerifier.scanSanitizesTaggedInput(
            path, DifcGrantPolicy.TAG_ORDER, "orders", "order-validations"));
  }

  @Test
  void joinRepublicationRetainsSensitiveOrderFields() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "egressTopic": "orders",
              "expressionTree": {
                "kind": "sink",
                "topic": "orders",
                "children": [{
                  "kind": "operator",
                  "topic": "join",
                  "children": [
                    {"kind": "scan", "topic": "orders"},
                    {"kind": "scan", "topic": "order-validations"}
                  ]
                }]
              }
            }]
          }
        }
        """);
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        policy.getRelationalAlgebraAnalysis().getProcessingPaths().get(0);
    assertFalse(
        ExpressionLineageVerifier.scanSanitizesTaggedInput(
            path, DifcGrantPolicy.TAG_ORDER, "orders", "orders"));
  }

  @Test
  void lineageLeakDetectionForDerivedNumeric() {
    final FieldLineage derived =
        new FieldLineage(
            "adjustedPrice",
            FieldLineage.ValueType.DOUBLE,
            java.util.Set.of("price"),
            "price * 1.1",
            FieldLineage.SanitizationKind.DERIVED);
    assertTrue(ExpressionLineageVerifier.lineageLeaksSensitive("price", derived));
    final FieldLineage bool =
        new FieldLineage(
            "isHighValue",
            FieldLineage.ValueType.BOOLEAN,
            java.util.Set.of("price"),
            "price >= 50",
            FieldLineage.SanitizationKind.BOOLEAN_PREDICATE);
    assertFalse(ExpressionLineageVerifier.lineageLeaksSensitive("price", bool));
  }

  private static AppProcessingPolicy readPolicy(final String json) throws Exception {
    return MAPPER.readValue(json, AppProcessingPolicy.class);
  }
}
