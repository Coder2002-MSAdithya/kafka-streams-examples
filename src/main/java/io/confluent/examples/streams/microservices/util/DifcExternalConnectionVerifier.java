package io.confluent.examples.streams.microservices.util;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/** Grantor-side {@code CAN_ADD} verification of attested non-Kafka socket endpoints. */
public final class DifcExternalConnectionVerifier {

  public static final String EXPECTED_PREFIX = "policy.grantor.expected.external.hosts.";

  private DifcExternalConnectionVerifier() {
  }

  public static DifcTagPolicyVerifier.VerificationResult verifyCanAdd(final String requesterPrincipal)
      throws IOException {
    final AppProcessingPolicy policy = DifcTagPolicyVerifier.loadRequesterPolicy(requesterPrincipal);
    return verifyCanAdd(requesterPrincipal, policy);
  }

  public static DifcTagPolicyVerifier.VerificationResult verifyCanAdd(
      final String requesterPrincipal,
      final byte[] policyBytes) throws IOException {
    final AppProcessingPolicy policy =
        DifcTagPolicyVerifier.loadRequesterPolicyFromBytes(requesterPrincipal, policyBytes);
    return verifyCanAdd(requesterPrincipal, policy);
  }

  private static DifcTagPolicyVerifier.VerificationResult verifyCanAdd(
      final String requesterPrincipal,
      final AppProcessingPolicy policy) {
    final Set<String> observed = observedEndpoints(policy);
    observed.removeAll(kafkaBootstrapEndpoints());
    final Set<String> expected = expectedEndpoints(requesterPrincipal);

    final Set<String> unexpected = new LinkedHashSet<>(observed);
    unexpected.removeAll(expected);
    if (!unexpected.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "unexpected external connection(s) "
              + unexpected
              + "; expected only "
              + (expected.isEmpty() ? "none (Kafka broker only)" : expected));
    }

    if (observed.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.allow(
          expected.isEmpty()
              ? "no external connections observed"
              : "no external connections observed yet; expected " + expected);
    }

    return DifcTagPolicyVerifier.VerificationResult.allow(
        "external connections " + observed + " match expected " + expected);
  }

  public static Set<String> expectedEndpoints(final String requesterPrincipal) {
    final Set<String> expected = new LinkedHashSet<>();
    final String raw = System.getProperty(EXPECTED_PREFIX + requesterPrincipal, "").trim();
    if (!raw.isEmpty()) {
      for (final String part : raw.split(",")) {
        final String normalized = normalizeEndpoint(part);
        if (!normalized.isEmpty()) {
          expected.add(normalized);
        }
      }
    }
    expected.addAll(ExpectedExternalConnectionsRegistry.defaultsForPrincipal(requesterPrincipal));
    return expected;
  }

  private static Set<String> observedEndpoints(final AppProcessingPolicy policy) {
    final Set<String> observed = new LinkedHashSet<>();
    if (policy == null) {
      return observed;
    }
    for (final AppProcessingPolicy.ExternalConnection connection : policy.getExternalConnections()) {
      if (connection.getEndpoint() != null && !connection.getEndpoint().isEmpty()) {
        observed.add(normalizeEndpoint(connection.getEndpoint()));
      }
    }
    return observed;
  }

  private static String normalizeEndpoint(final String raw) {
    if (raw == null) {
      return "";
    }
    final String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return "";
    }
    final int colon = trimmed.lastIndexOf(':');
    if (colon <= 0 || colon == trimmed.length() - 1) {
      return trimmed.toLowerCase();
    }
    return trimmed.substring(0, colon).toLowerCase() + ":" + trimmed.substring(colon + 1);
  }

  static Set<String> kafkaBootstrapEndpoints() {
    final Set<String> endpoints = new LinkedHashSet<>();
    appendBootstrapList(endpoints, System.getProperty("policy.agent.kafka.bootstrap", ""));
    appendBootstrapList(endpoints, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", ""));
    expandLocalhostAliases(endpoints);
    return endpoints;
  }

  private static void expandLocalhostAliases(final Set<String> endpoints) {
    final Set<String> aliases = new LinkedHashSet<>();
    for (final String endpoint : endpoints) {
      if (endpoint.startsWith("localhost:")) {
        aliases.add("127.0.0.1:" + endpoint.substring("localhost:".length()));
      } else if (endpoint.startsWith("127.0.0.1:")) {
        aliases.add("localhost:" + endpoint.substring("127.0.0.1:".length()));
      }
    }
    endpoints.addAll(aliases);
  }

  private static void appendBootstrapList(final Set<String> endpoints, final String raw) {
    if (raw == null || raw.isBlank()) {
      return;
    }
    for (final String part : raw.split(",")) {
      final String normalized = normalizeEndpoint(part);
      if (!normalized.isEmpty()) {
        endpoints.add(normalized);
      }
    }
  }
}
