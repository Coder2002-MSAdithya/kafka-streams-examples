package io.confluent.examples.streams.microservices.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Publishes agent-attested processing policies for grantors to verify before {@code CAN_REMOVE}.
 */
public final class DifcPolicyPublisher {

  private DifcPolicyPublisher() {
  }

  public static String resolveRequesterPrincipal() {
    final String principal = System.getProperty("policy.app.principal");
    if (principal == null || principal.isEmpty()) {
      throw new IllegalStateException(
          "policy.app.principal JVM property is required to publish attested processing policy");
    }
    return principal;
  }

  /**
   * Ensures the requester's attested policy is present in the shared registry before
   * {@code GRANT_CAP} for {@code CAN_REMOVE} is sent.
   */
  public static void publishAttestedPolicyForGrant() throws IOException {
    final String requesterPrincipal = resolveRequesterPrincipal();
    final Path registryPath = DifcTagPolicyVerifier.policyPathForRequester(requesterPrincipal);
    if (!Files.isRegularFile(registryPath)) {
      throw new IOException("attested policy not found at " + registryPath);
    }
    final AttestedProcessingPolicy attestation = AttestedProcessingPolicy.readFromFile(registryPath);
    if (attestation == null) {
      throw new IOException("failed to parse attested policy at " + registryPath);
    }
    final Path submissionPath = registryPath.resolveSibling("processing-policy-submission.json");
    Files.copy(registryPath, submissionPath, StandardCopyOption.REPLACE_EXISTING);
    System.out.printf(
        "[DIFC] policyAttestationPublished requester=%s format=%s path=%s%n",
        requesterPrincipal,
        attestation.getFormat(),
        submissionPath);
  }
}
