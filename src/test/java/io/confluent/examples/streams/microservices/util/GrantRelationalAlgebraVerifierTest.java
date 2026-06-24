package io.confluent.examples.streams.microservices.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrantRelationalAlgebraVerifierTest {

  @org.junit.jupiter.api.AfterEach
  void clearGrantSanitizationMode() {
    System.clearProperty("difc.grant.sanitization.mode");
  }

  @Test
  void incompleteGraphRepublicationStillDeniesOrderTag() throws Exception {
    final AppProcessingPolicy policy = AgentPolicyTestSupport.agentEnriched("""
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
    assertFalse(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "orders", policy));
  }

  @Test
  void treeJoinDeniesWhenScanOrdersRetainsSensitiveFields() throws Exception {
    final AppProcessingPolicy policy = AgentPolicyTestSupport.agentEnriched("""
        {
          "version": 2,
          "sources": ["orders", "order-validations"],
          "egressPaths": [
            {
              "topic": "orders",
              "ingressTopics": ["orders", "order-validations"],
              "operators": ["join", "aggregate"]
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
              {"from":"topic_orders","to":"op_join","label":"left"},
              {"from":"op_join","to":"topic_orders","label":"writes"}
            ]
          }
        }
        """);
    final AppProcessingPolicy.ProcessingPathAnalysis path =
        GrantRelationalAlgebraVerifier.pathAnalysisFor(policy, "orders", "orders");
    assertTrue(path != null);
    assertTrue(path.getAlgebraExpression().contains("Scan(orders)"));
    assertTrue(path.getAlgebraExpression().contains("Scan(order-validations)"));
    assertFalse(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER,
            "orders",
            "orders",
            policy));
    assertTrue(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_FRAUD,
            "order-validations",
            "orders",
            policy));
  }

  @Test
  void fraudSplitMergeValidationSinkGrantsOrderTagRemoval() throws Exception {
    final AppProcessingPolicy policy = AgentPolicyTestSupport.agentEnriched("""
        {
          "version": 2,
          "principal": "fraud-svc",
          "sources": ["orders"],
          "aggregations": ["aggregate", "groupBy", "windowedBy"],
          "egressPaths": [
            {
              "topic": "order-validations",
              "ingressTopics": ["orders"],
              "operators": [
                "filter", "groupBy", "windowedBy", "aggregate",
                "split", "mapValues", "merge"
              ],
              "declassifyTags": ["order"],
              "callbackProjections": [
                {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]},
                {"operator":"merge","outputFields":[]}
              ]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"op_split","kind":"operator","label":"split()"},
              {"id":"op_merge","kind":"operator","label":"merge()"},
              {"id":"topic_validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"op_merge","label":"input"},
              {"from":"op_merge","to":"topic_validations","label":"writes"}
            ]
          }
        }
        """);
    assertTrue(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "order-validations", policy));
  }

  @Test
  void validatorsGrantOrderTagRemovalWhileAggregatorDeniesRepublication() throws Exception {
    final AppProcessingPolicy fraud = AgentPolicyTestSupport.agentEnriched("""
        {
          "version": 2,
          "principal": "fraud-svc",
          "sources": ["orders"],
          "egressPaths": [{
            "topic": "order-validations",
            "ingressTopics": ["orders"],
            "operators": ["filter", "groupBy", "windowedBy", "aggregate", "split", "mapValues", "merge"],
            "declassifyTags": ["order"],
            "callbackProjections": [
              {"operator":"mapValues","outputFields":["orderId","checkType","validationResult"]}
            ]
          }],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"op_merge","kind":"operator","label":"merge()"},
              {"id":"topic_validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"op_merge","label":"input"},
              {"from":"op_merge","to":"topic_validations","label":"writes"}
            ]
          }
        }
        """);
    final AppProcessingPolicy inventory = AgentPolicyTestSupport.agentEnriched("""
        {
          "version": 2,
          "principal": "inventory-svc",
          "sources": ["orders", "warehouse-inventory"],
          "egressPaths": [{
            "topic": "order-validations",
            "ingressTopics": ["orders", "warehouse-inventory"],
            "operators": ["selectKey", "filter", "join", "process"],
            "declassifyTags": ["order"],
            "callbackProjections": [
              {"operator":"process","outputFields":["orderId","checkType","validationResult"]}
            ]
          }],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"topic_inventory","kind":"topic","topic":"warehouse-inventory"},
              {"id":"op_join","kind":"operator","label":"join()"},
              {"id":"op_process","kind":"operator","label":"process()"},
              {"id":"topic_validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"op_join","label":"left"},
              {"from":"topic_inventory","to":"op_join","label":"right"},
              {"from":"op_join","to":"op_process","label":"input"},
              {"from":"op_process","to":"topic_validations","label":"writes"}
            ]
          }
        }
        """);
    final AppProcessingPolicy aggregator = AgentPolicyTestSupport.agentEnriched("""
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
              {"from":"topic_orders","to":"op_join","label":"left"},
              {"from":"op_join","to":"topic_orders","label":"writes"}
            ]
          }
        }
        """);
    assertTrue(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "order-validations", fraud));
    assertTrue(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "order-validations", inventory));
    assertFalse(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "orders", aggregator));
  }

  @Test
  void favorableSinkColumnsCannotBypassDerivedLineageLeak() throws Exception {
    final AppProcessingPolicy policy = AgentPolicyTestSupport.agentEnriched("""
        {
          "version": 2,
          "sources": ["orders"],
          "egressPaths": [{
            "topic": "order-validations",
            "ingressTopics": ["orders"],
            "operators": ["mapValues"],
            "declassifyTags": ["order"]
          }],
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "egressTopic": "order-validations",
              "ingressTopic": "orders",
              "outputFields": ["orderId", "checkType", "validationResult"],
              "droppedSensitiveFields": ["customerId", "price", "quantity", "product", "card", "state"],
              "expressionTree": {
                "kind": "sink",
                "topic": "order-validations",
                "children": [{
                  "kind": "operator",
                  "topic": "mapValues",
                  "algebraSymbol": "π",
                  "outputFields": ["orderId", "checkType", "validationResult"],
                  "fieldLineages": [
                    {
                      "outputField": "orderId",
                      "sourceFields": ["id"],
                      "expression": "id",
                      "sanitizationKind": "PASSTHROUGH"
                    },
                    {
                      "outputField": "checkType",
                      "sourceFields": [],
                      "expression": "FRAUD_CHECK",
                      "sanitizationKind": "CONSTANT"
                    },
                    {
                      "outputField": "validationResult",
                      "sourceFields": ["price", "quantity"],
                      "expression": "price * quantity",
                      "sanitizationKind": "DERIVED"
                    }
                  ],
                  "children": [{"kind": "scan", "topic": "orders"}]
                }]
              }
            }]
          }
        }
        """);
    assertFalse(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "order-validations", policy));
  }

  @Test
  void taintModeGrantsWithoutExpressionTreeWhenSensitiveFieldsAreSanitized() throws Exception {
    final AppProcessingPolicy policy = AppProcessingPolicy.readFromCanonicalJson("""
        {
          "version": 2,
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "ingressTopic": "orders",
              "egressTopic": "order-validations",
              "sourceFieldTaint": [
                {"sourceTopic":"orders","sourceField":"customerId","status":"DROPPED","sanitized":true},
                {"sourceTopic":"orders","sourceField":"product","status":"SANITIZED_AGGREGATE","sanitized":true},
                {"sourceTopic":"orders","sourceField":"quantity","status":"SANITIZED_WINDOWED_AGGREGATE","sanitized":true},
                {"sourceTopic":"orders","sourceField":"price","status":"SANITIZED_BOOLEAN","sanitized":true},
                {"sourceTopic":"orders","sourceField":"state","status":"DROPPED","sanitized":true}
              ]
            }]
          }
        }
        """);
    System.setProperty("difc.grant.sanitization.mode", "taint");
    assertTrue(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "order-validations", policy));
  }

  @Test
  void taintModeDeniesWhenAnySensitiveFieldIsUnsanitized() throws Exception {
    final AppProcessingPolicy policy = AppProcessingPolicy.readFromCanonicalJson("""
        {
          "version": 2,
          "relationalAlgebraAnalysis": {
            "processingPaths": [{
              "ingressTopic": "orders",
              "egressTopic": "order-validations",
              "sourceFieldTaint": [
                {"sourceTopic":"orders","sourceField":"customerId","status":"DROPPED","sanitized":true},
                {"sourceTopic":"orders","sourceField":"product","status":"SANITIZED_AGGREGATE","sanitized":true},
                {"sourceTopic":"orders","sourceField":"quantity","status":"SANITIZED_WINDOWED_AGGREGATE","sanitized":true},
                {"sourceTopic":"orders","sourceField":"price","status":"UNSANITIZED_PASSTHROUGH","sanitized":false},
                {"sourceTopic":"orders","sourceField":"state","status":"DROPPED","sanitized":true}
              ]
            }]
          }
        }
        """);
    System.setProperty("difc.grant.sanitization.mode", "taint");
    assertFalse(
        GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            DifcGrantPolicy.TAG_ORDER, "orders", "order-validations", policy));
  }
}
