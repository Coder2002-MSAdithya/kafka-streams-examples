package io.confluent.examples.streams.microservices.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Signature;
import java.security.cert.CertPath;
import java.security.cert.CertPathValidator;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Verifies ECDSA signatures and certificate chains on agent-attested processing policies.
 */
public final class PolicyAttestationVerifier {

  public static final String TRUSTED_CA_PATH_PROP = "policy.grantor.trusted.ca.path";
  private static final String FALLBACK_CA_PATH_PROP = "policy.agent.trusted.ca.path";

  private PolicyAttestationVerifier() {
  }

  public static DifcTagPolicyVerifier.VerificationResult verifyAttestation(
      final String requesterPrincipal,
      final AttestedProcessingPolicy attestation) {
    Objects.requireNonNull(requesterPrincipal, "requesterPrincipal");
    if (attestation == null) {
      return DifcTagPolicyVerifier.VerificationResult.deny("no attested processing-policy for requester");
    }
    final DifcTagPolicyVerifier.VerificationResult formatCheck = validateEnvelopeFormat(attestation);
    if (!formatCheck.allowed()) {
      return formatCheck;
    }

    try {
      final byte[] payload = Base64.getDecoder().decode(attestation.getSignedPayloadBase64());
      final byte[] signatureBytes = Base64.getDecoder().decode(attestation.getSignatureBase64());
      final X509Certificate signerCert = parseCertificate(attestation.getCertificatePem());

      if (!verifyCertificateChain(signerCert)) {
        return DifcTagPolicyVerifier.VerificationResult.deny(
            "untrusted policy signing certificate chain");
      }
      if (!verifySignature(attestation, signerCert, payload, signatureBytes)) {
        return DifcTagPolicyVerifier.VerificationResult.deny("invalid ECDSA policy signature");
      }

      final AppProcessingPolicy policy =
          AppProcessingPolicy.readFromCanonicalJson(new String(payload, StandardCharsets.UTF_8));
      return validateSignedPolicyPrincipal(requesterPrincipal, policy);
    } catch (final Exception e) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "policy attestation verification failed: " + e.getMessage());
    }
  }

  public static LoadedAttestedPolicy loadAndVerifyAttestation(final String requesterPrincipal)
      throws IOException {
    final Path path = DifcTagPolicyVerifier.policyPathForRequester(requesterPrincipal);
    final AttestedProcessingPolicy attestation = AttestedProcessingPolicy.readFromFile(path);
    final DifcTagPolicyVerifier.VerificationResult crypto =
        verifyAttestation(requesterPrincipal, attestation);
    if (!crypto.allowed()) {
      return new LoadedAttestedPolicy(null, crypto);
    }
    final byte[] payload = Base64.getDecoder().decode(attestation.getSignedPayloadBase64());
    final AppProcessingPolicy policy =
        AppProcessingPolicy.readFromCanonicalJson(new String(payload, StandardCharsets.UTF_8));
    return new LoadedAttestedPolicy(policy, crypto);
  }

  private static DifcTagPolicyVerifier.VerificationResult validateEnvelopeFormat(
      final AttestedProcessingPolicy attestation) {
    if (!AttestedProcessingPolicy.SIGNED_FORMAT.equals(attestation.getFormat())) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "policy attestation missing or unsigned (format=" + attestation.getFormat() + ")");
    }
    if (attestation.getSignedPayloadBase64() == null
        || attestation.getSignatureBase64() == null
        || attestation.getCertificatePem() == null) {
      return DifcTagPolicyVerifier.VerificationResult.deny("incomplete policy attestation envelope");
    }
    return DifcTagPolicyVerifier.VerificationResult.allow();
  }

  private static boolean verifySignature(
      final AttestedProcessingPolicy attestation,
      final X509Certificate signerCert,
      final byte[] payload,
      final byte[] signatureBytes) throws Exception {
    final String algorithm = attestation.getSignatureAlgorithm() == null
        ? "SHA256withECDSA"
        : attestation.getSignatureAlgorithm();
    final Signature signature = Signature.getInstance(algorithm);
    signature.initVerify(signerCert.getPublicKey());
    signature.update(payload);
    return signature.verify(signatureBytes);
  }

  private static DifcTagPolicyVerifier.VerificationResult validateSignedPolicyPrincipal(
      final String requesterPrincipal,
      final AppProcessingPolicy policy) {
    if (policy == null) {
      return DifcTagPolicyVerifier.VerificationResult.deny("signed policy payload is not valid JSON");
    }
    if (policy.getPrincipal() == null || !requesterPrincipal.equals(policy.getPrincipal())) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "signed policy principal "
              + policy.getPrincipal()
              + " does not match requester "
              + requesterPrincipal);
    }
    return DifcTagPolicyVerifier.VerificationResult.allow();
  }

  private static X509Certificate parseCertificate(final String pem) throws Exception {
    final CertificateFactory factory = CertificateFactory.getInstance("X.509");
    return (X509Certificate) factory.generateCertificate(
        new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8)));
  }

  private static boolean verifyCertificateChain(final X509Certificate leaf) throws Exception {
    final Set<TrustAnchor> anchors = loadTrustAnchors();
    if (anchors.isEmpty()) {
      throw new IllegalStateException("no trusted CA anchors configured for policy attestation");
    }
    final CertificateFactory factory = CertificateFactory.getInstance("X.509");
    final CertPath path = factory.generateCertPath(List.of(leaf));
    final PKIXParameters params = new PKIXParameters(anchors);
    params.setRevocationEnabled(false);
    CertPathValidator.getInstance("PKIX").validate(path, params);
    return true;
  }

  private static Set<TrustAnchor> loadTrustAnchors() throws Exception {
    final String configured = System.getProperty(TRUSTED_CA_PATH_PROP);
    final String fallback = System.getProperty(FALLBACK_CA_PATH_PROP);
    final String path = configured != null && !configured.isEmpty() ? configured : fallback;
    if (path == null || path.isEmpty()) {
      return Collections.emptySet();
    }
    final CertificateFactory factory = CertificateFactory.getInstance("X.509");
    final Set<TrustAnchor> anchors = new LinkedHashSet<>();
    try (ByteArrayInputStream in = new ByteArrayInputStream(Files.readAllBytes(Paths.get(path)))) {
      for (final Certificate certificate : factory.generateCertificates(in)) {
        anchors.add(new TrustAnchor((X509Certificate) certificate, null));
      }
    }
    return anchors;
  }

  public record LoadedAttestedPolicy(
      AppProcessingPolicy policy,
      DifcTagPolicyVerifier.VerificationResult attestationResult) {}
}
