package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.Capability;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DifcTagPolicyVerifierTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void fraudConsumingOrdersRequiresSanitizedGraph() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders"],
          "aggregations": ["merge"],
          "egressPaths": [
            {
              "topic": "order-validations",
              "ingressTopics": ["orders"],
              "operators": ["aggregate", "merge", "split"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"stream_1","kind":"stream","label":"KStream"},
              {"id":"op_filter","kind":"operator","label":"filter()"},
              {"id":"op_map","kind":"operator","label":"mapValues()"},
              {"id":"op_merge","kind":"operator","label":"merge()"},
              {"id":"topic_order_validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"stream_1","label":"source"},
              {"from":"stream_1","to":"op_filter","label":"input"},
              {"from":"op_filter","to":"op_map","label":"output"},
              {"from":"op_map","to":"op_merge","label":"input"},
              {"from":"op_merge","to":"topic_order_validations","label":"writes"}
            ]
          }
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertTrue(result.allowed(), result.reason());
    assertTrue(result.reason().contains("policy verified"));
  }

  @Test
  void aggregatorConsumingValidationTopicsVerifiedByValidatorGrantor() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders", "order-validations"],
          "aggregations": ["join", "aggregate", "merge"],
          "egressPaths": [
            {
              "topic": "orders",
              "ingressTopics": ["orders", "order-validations"],
              "operators": ["join", "aggregate", "merge", "to"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"topic_order_validations","kind":"topic","topic":"order-validations"},
              {"id":"op_join","kind":"operator","label":"join()"}
            ],
            "edges": [
              {"from":"topic_order_validations","to":"op_join","label":"source"},
              {"from":"topic_orders","to":"op_join","label":"left"},
              {"from":"op_join","to":"topic_orders","label":"writes"}
            ]
          }
        }
        """);
    assertTrue(
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_FRAUD,
            DifcGrantPolicy.PRINCIPAL_FRAUD,
            policy).allowed());
    assertFalse(
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy).allowed());
  }

  @Test
  void noGrantorTopicConsumptionIsVacuous() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["warehouse-inventory"],
          "egressPaths": [
            {
              "topic": "order-validations",
              "ingressTopics": ["warehouse-inventory"],
              "operators": ["join"]
            }
          ]
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertTrue(result.allowed(), result.reason());
    assertTrue(result.reason().contains("vacuous"));
  }

  @Test
  void collapsedExpressionWithAgentPathMetricsStillGrants() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders"],
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "ingressTopic": "orders",
              "egressTopic": "order-validations",
              "algebraExpression": "Sink(order-validations, π(∪(Scan(orders))))",
              "inputFields": ["customerId", "state", "product", "price", "quantity", "id"],
              "outputFields": ["orderId", "checkType", "validationResult"],
              "droppedSensitiveFields": ["customerId", "state", "product", "price", "quantity"],
              "schemaChanged": true,
              "sensitiveFieldSanitizationRatio": 0.83,
              "expressionTree": {
                "kind": "sink",
                "topic": "order-validations",
                "children": [{
                  "kind": "operator",
                  "topic": "merge",
                  "children": [{"kind": "scan", "topic": "orders"}]
                }]
              }
            }]
          },
          "egressPaths": [{
            "topic": "order-validations",
            "ingressTopics": ["orders"],
            "operators": ["mapValues", "merge"],
            "declassifyTags": ["order"]
          }]
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertTrue(result.allowed(), result.reason());
  }

  @Test
  void fraudManifestGraphDocumentsOrdersToValidationsPath() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "principal": "fraud-svc",
          "service": "FraudService",
          "sources": ["orders"],
          "aggregations": ["aggregate", "groupBy", "merge", "split", "windowedBy"],
          "egressPaths": [
            {
              "topic": "order-validations",
              "ingressTopics": [],
              "operators": ["mapValues", "merge", "filter", "aggregate"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"stream_1","kind":"stream","label":"KStream"},
              {"id":"op_merge","kind":"operator","label":"merge()"},
              {"id":"topic_order-validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"stream_1","label":"source"},
              {"from":"op_merge","to":"topic_order-validations","label":"writes"}
            ]
          }
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertTrue(result.allowed(), result.reason());
    assertTrue(result.reason().contains("policy verified"));
  }

  @Test
  void sameTopicRepublicationRequiresOperatorLoopInGraph() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders", "order-validations"],
          "egressPaths": [
            {
              "topic": "orders",
              "ingressTopics": ["orders", "order-validations"],
              "operators": ["join", "aggregate", "merge"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"topic_order_validations","kind":"topic","topic":"order-validations"},
              {"id":"op_join","kind":"operator","label":"join()"}
            ],
            "edges": [
              {"from":"topic_order_validations","to":"op_join","label":"right"},
              {"from":"op_join","to":"topic_orders","label":"writes"}
            ]
          }
        }
        """);
    assertTrue(
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_FRAUD,
            DifcGrantPolicy.PRINCIPAL_FRAUD,
            policy).allowed());
    final DifcTagPolicyVerifier.VerificationResult repubOrderResult =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertFalse(repubOrderResult.allowed(), repubOrderResult.reason());
  }

  @Test
  void aggregatorManifestGraphDocumentsOrdersRepublicationLoop() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "principal": "validations-agg-svc",
          "service": "ValidationsAggregatorService",
          "sources": ["orders", "order-validations"],
          "aggregations": ["join", "aggregate", "merge"],
          "egressPaths": [
            {
              "topic": "orders",
              "ingressTopics": ["orders", "order-validations"],
              "operators": ["join", "aggregate", "merge", "to"]
            }
          ],
          "graph": {"nodes": [], "edges": []}
        }
        """);
    assertFalse(
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy).allowed());
    assertTrue(
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_FRAUD,
            DifcGrantPolicy.PRINCIPAL_FRAUD,
            policy).allowed());
  }

  @Test
  void joinOnlyOrdersPathFailsFieldReducingCheck() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders"],
          "egressPaths": [
            {
              "topic": "orders",
              "ingressTopics": ["orders"],
              "operators": ["join"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"op_join","kind":"operator","label":"join()"}
            ],
            "edges": [
              {"from":"topic_orders","to":"op_join","label":"left"},
              {"from":"op_join","to":"topic_orders","label":"writes"}
            ]
          }
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertFalse(result.allowed());
    assertTrue(result.reason().contains("retains grantor-sensitive fields"));
  }

  @Test
  void everyOutputRelationMustSanitizeTaggedInput() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders"],
          "egressPaths": [
            {
              "topic": "order-validations",
              "ingressTopics": ["orders"],
              "operators": ["filter", "mapValues"]
            },
            {
              "topic": "orders",
              "ingressTopics": ["orders"],
              "operators": ["join"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"op_filter","kind":"operator","label":"filter()"},
              {"id":"op_map","kind":"operator","label":"mapValues()"},
              {"id":"topic_order_validations","kind":"topic","topic":"order-validations"},
              {"id":"op_join","kind":"operator","label":"join()"}
            ],
            "edges": [
              {"from":"topic_orders","to":"op_filter","label":"source"},
              {"from":"op_filter","to":"op_map","label":"output"},
              {"from":"op_map","to":"topic_order_validations","label":"writes"},
              {"from":"topic_orders","to":"op_join","label":"left"},
              {"from":"op_join","to":"topic_orders","label":"writes"}
            ]
          }
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertFalse(result.allowed(), result.reason());
    assertTrue(result.reason().contains("orders → orders"));
  }

  @Test
  void consumedGrantorTopicWithoutSanitizationDenies() throws Exception {
    final AppProcessingPolicy policy = readPolicy("""
        {
          "version": 2,
          "sources": ["orders"],
          "egressPaths": [
            {
              "topic": "order-validations",
              "ingressTopics": ["orders"],
              "operators": []
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"topic_order_validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"topic_order_validations","label":"writes"}
            ]
          }
        }
        """);
    final DifcTagPolicyVerifier.VerificationResult passthroughResult =
        DifcTagPolicyVerifier.verifyCanRemoveOnTag(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            policy);
    assertFalse(passthroughResult.allowed(), passthroughResult.reason());
    assertFalse(
        DifcTagPolicyVerifier.isAllowedGrantWithPolicy(
            DifcGrantPolicy.TAG_ORDER,
            DifcGrantPolicy.PRINCIPAL_ORDERS,
            DifcGrantPolicy.PRINCIPAL_FRAUD,
            Capability.CAN_REMOVE,
            policy));
  }

  private static AppProcessingPolicy readPolicy(final String json) throws Exception {
    return AgentPolicyTestSupport.agentEnriched(json);
  }
}
