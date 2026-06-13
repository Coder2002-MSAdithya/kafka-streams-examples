package io.confluent.examples.streams.microservices.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrantRelationalAlgebraVerifierTest {

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
}
