package io.confluent.examples.streams.microservices.util;

import org.apache.kafka.clients.Capability;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Verifies agent-attested {@link AppProcessingPolicy} documents before granting {@code CAN_REMOVE}.
 * <p>
 * The Java agent exports {@code relationalAlgebraAnalysis} in the signed policy payload; the grantor
 * only checks attestation and pre-computed field-sanitization metrics (no re-extraction).
 * <p>
 * {@code CAN_ADD} grants require only {@link DifcGrantPolicy#isPrincipalAllowedGrant}.
 */
public final class DifcTagPolicyVerifier {

  public static final String POLICY_REGISTRY_DIR_PROP = "policy.registry.dir";

  private DifcTagPolicyVerifier() {
  }

  public static Path policyPathForRequester(final String requesterPrincipal) {
    final String registryDir = System.getProperty(POLICY_REGISTRY_DIR_PROP, "/tmp/kafka-streams-examples/policy");
    return Paths.get(registryDir, requesterPrincipal, "processing-policy.json");
  }

  public static AppProcessingPolicy loadRequesterPolicy(final String requesterPrincipal) throws IOException {
    final PolicyAttestationVerifier.LoadedAttestedPolicy loaded =
        PolicyAttestationVerifier.loadAndVerifyAttestation(requesterPrincipal);
    if (!loaded.attestationResult().allowed()) {
      throw new IOException(loaded.attestationResult().reason());
    }
    return loaded.policy();
  }

  public static VerificationResult verifyAttestationAndCanRemoveOnTag(
      final String requesterPrincipal,
      final String grantorPrincipal,
      final String ownedTag) throws IOException {
    final PolicyAttestationVerifier.LoadedAttestedPolicy loaded =
        PolicyAttestationVerifier.loadAndVerifyAttestation(requesterPrincipal);
    if (!loaded.attestationResult().allowed()) {
      return loaded.attestationResult();
    }
    return verifyCanRemoveOnTag(ownedTag, grantorPrincipal, loaded.policy());
  }

  public static VerificationResult verifyCanRemoveOnTag(
      final String ownedTag,
      final String grantorPrincipal,
      final AppProcessingPolicy policy) {
    Objects.requireNonNull(ownedTag, "ownedTag");
    Objects.requireNonNull(grantorPrincipal, "grantorPrincipal");
    return ProcessingPolicyGraphAnalyzer.verifyCanRemoveOnTag(ownedTag, grantorPrincipal, policy);
  }

  public static boolean isAllowedGrantWithPolicy(
      final String tagName,
      final String grantorPrincipal,
      final String requesterPrincipal,
      final Capability capability,
      final AppProcessingPolicy requesterPolicy) {
    if (!DifcGrantPolicy.isPrincipalAllowedGrant(tagName, requesterPrincipal, capability)) {
      return false;
    }
    if (capability != Capability.CAN_REMOVE) {
      return true;
    }
    return verifyCanRemoveOnTag(tagName, grantorPrincipal, requesterPolicy).allowed();
  }

  public record VerificationResult(boolean allowed, String reason) {
    public static VerificationResult allow() {
      return new VerificationResult(true, "policy verified");
    }

    public static VerificationResult allow(final String reason) {
      return new VerificationResult(true, reason);
    }

    public static VerificationResult deny(final String reason) {
      return new VerificationResult(false, reason);
    }
  }
}
