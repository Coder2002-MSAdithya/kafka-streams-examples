package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProcessingPolicyGraphHelperTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void derivesIngressTopicsFromGraph() throws Exception {
    final AppProcessingPolicy policy = MAPPER.readValue("""
        {
          "version": 2,
          "egressPaths": [{"topic": "order-validations", "ingressTopics": []}],
          "graph": {
            "nodes": [
              {"id":"topic_orders","kind":"topic","topic":"orders"},
              {"id":"stream_1","kind":"stream","label":"KStream"},
              {"id":"op_to","kind":"operator","label":"to()"},
              {"id":"topic_order_validations","kind":"topic","topic":"order-validations"}
            ],
            "edges": [
              {"from":"topic_orders","to":"stream_1","label":"source"},
              {"from":"stream_1","to":"op_to","label":"input"},
              {"from":"op_to","to":"topic_order_validations","label":"writes"}
            ]
          }
        }
        """, AppProcessingPolicy.class);

    ProcessingPolicyGraphHelper.enrichEgressPathsFromGraph(policy);
    assertEquals(1, policy.getEgressPaths().size());
    assertTrue(policy.getEgressPaths().get(0).getIngressTopics().contains("orders"));
  }

  @Test
  void classifiesPassthroughAndFieldReducingOperators() {
    assertTrue(ProcessingPolicyGraphHelper.isFieldReducingOperator("mapValues"));
    assertTrue(ProcessingPolicyGraphHelper.isFieldReducingOperator("filter"));
    assertTrue(ProcessingPolicyGraphHelper.isFieldReducingOperator("aggregate"));
    assertFalse(ProcessingPolicyGraphHelper.isFieldReducingOperator("join"));
    assertFalse(ProcessingPolicyGraphHelper.isFieldReducingOperator("merge"));
  }

  @Test
  void enrichesIngressFromDeclaredSources() throws Exception {
    final AppProcessingPolicy policy = MAPPER.readValue("""
        {
          "version": 2,
          "sources": ["orders", "warehouse-inventory"],
          "egressPaths": [{"topic": "order-validations", "ingressTopics": []}],
          "graph": {"nodes": [], "edges": []}
        }
        """, AppProcessingPolicy.class);

    ProcessingPolicyGraphHelper.enrichEgressPathsFromSources(policy);
    assertTrue(policy.getEgressPaths().get(0).getIngressTopics().contains("orders"));
    assertTrue(policy.getEgressPaths().get(0).getIngressTopics().contains("warehouse-inventory"));
  }

  @Test
  void computesAggregationMetricsFromGraph() throws Exception {
    final AppProcessingPolicy policy = MAPPER.readValue("""
        {
          "version": 2,
          "aggregations": ["join"],
          "egressPaths": [
            {
              "topic": "orders",
              "ingressTopics": ["orders", "order-validations"],
              "operators": ["join", "merge"]
            }
          ],
          "graph": {
            "nodes": [
              {"id":"op_join","kind":"operator","label":"join()"},
              {"id":"op_branch","kind":"operator","label":"branch()"}
            ],
            "edges": []
          }
        }
        """, AppProcessingPolicy.class);

    final AppProcessingPolicy.AggregationAnalysis analysis =
        ProcessingPolicyGraphHelper.computeAggregationAnalysis(policy);
    assertTrue(analysis.getJoinCount() >= 1);
    assertTrue(analysis.getBranchCount() >= 1);
    assertEquals(1, analysis.getMultiSourceEgressPaths());
  }
}
