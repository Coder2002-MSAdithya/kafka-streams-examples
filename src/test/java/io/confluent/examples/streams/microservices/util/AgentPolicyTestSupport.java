package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.security.agent.policy.ProcessingPolicyEnricher;

/**
 * Simulates agent-side policy enrichment in unit tests. Production policies arrive pre-enriched
 * from the DIFC policy Java agent ({@code -javaagent:policy-agent.jar}).
 */
final class AgentPolicyTestSupport {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AgentPolicyTestSupport() {
  }

  static AppProcessingPolicy agentEnriched(final String json) throws Exception {
    final org.apache.kafka.security.agent.policy.AppProcessingPolicy agentPolicy =
        MAPPER.readValue(json, org.apache.kafka.security.agent.policy.AppProcessingPolicy.class);
    ProcessingPolicyEnricher.enrich(agentPolicy);
    return MAPPER.readValue(MAPPER.writeValueAsString(agentPolicy), AppProcessingPolicy.class);
  }
}
