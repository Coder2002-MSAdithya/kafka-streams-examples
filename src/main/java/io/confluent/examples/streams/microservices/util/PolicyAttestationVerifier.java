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
  private static final String TRUSTED_CA_PATH_ENV = "POLICY_GRANTOR_TRUSTED_CA_PATH";
  private static final String FALLBACK_CA_PATH_ENV = "POLICY_AGENT_TRUSTED_CA_PATH";

  private PolicyAttestationVerifier() {
  }

  public static DifcTagPolicyVerifier.VerificationResult verifyAttestation(
      final String requesterPrincipal,
      final AttestedProcessingPolicy attestation) {
    Objects.requireNonNull(requesterPrincipal, "requesterPrincipal");
    System.out.printf(
        "[DIFC] attestationVerify requester=%s phase=start format=%s%n",
        requesterPrincipal, attestation == null ? "null" : attestation.getFormat());
    if (attestation == null) {
      return DifcTagPolicyVerifier.VerificationResult.deny("no attested processing-policy for requester");
    }
    final DifcTagPolicyVerifier.VerificationResult formatCheck = validateEnvelopeFormat(attestation);
    if (!formatCheck.allowed()) {
      System.out.printf(
          "[DIFC] attestationVerify requester=%s phase=format-check ok=false reason=\"%s\"%n",
          requesterPrincipal, formatCheck.reason());
      return formatCheck;
    }
    System.out.printf(
        "[DIFC] attestationVerify requester=%s phase=format-check ok=true%n",
        requesterPrincipal);

    try {
      final byte[] payload = Base64.getDecoder().decode(attestation.getSignedPayloadBase64());
      final byte[] signatureBytes = Base64.getDecoder().decode(attestation.getSignatureBase64());
      final X509Certificate signerCert = parseCertificate(attestation.getCertificatePem());

      if (!verifyCertificateChain(signerCert)) {
        System.out.printf(
            "[DIFC] attestationVerify requester=%s phase=cert-chain ok=false%n",
            requesterPrincipal);
        return DifcTagPolicyVerifier.VerificationResult.deny(
            "untrusted policy signing certificate chain");
      }
      System.out.printf(
          "[DIFC] attestationVerify requester=%s phase=cert-chain ok=true subject=\"%s\"%n",
          requesterPrincipal, signerCert.getSubjectX500Principal().getName());

      if (!verifySignature(attestation, signerCert, payload, signatureBytes)) {
        System.out.printf(
            "[DIFC] attestationVerify requester=%s phase=ecdsa-sig ok=false%n",
            requesterPrincipal);
        return DifcTagPolicyVerifier.VerificationResult.deny("invalid ECDSA policy signature");
      }
      System.out.printf(
          "[DIFC] attestationVerify requester=%s phase=ecdsa-sig ok=true algorithm=%s%n",
          requesterPrincipal,
          attestation.getSignatureAlgorithm() != null
              ? attestation.getSignatureAlgorithm()
              : "SHA256withECDSA");

      final AppProcessingPolicy policy =
          AppProcessingPolicy.readFromCanonicalJson(new String(payload, StandardCharsets.UTF_8));
      final DifcTagPolicyVerifier.VerificationResult principalCheck =
          validateSignedPolicyPrincipal(requesterPrincipal, policy);
      System.out.printf(
          "[DIFC] attestationVerify requester=%s phase=principal-match ok=%s%n",
          requesterPrincipal, principalCheck.allowed());
      return principalCheck;
    } catch (final Exception e) {
      System.out.printf(
          "[DIFC] attestationVerify requester=%s phase=exception msg=\"%s\"%n",
          requesterPrincipal, e.getMessage());
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "policy attestation verification failed: " + e.getMessage());
    }
  }

  public static LoadedAttestedPolicy loadAndVerifyAttestation(final String requesterPrincipal)
      throws IOException {
    final Path path = DifcTagPolicyVerifier.policyPathForRequester(requesterPrincipal);
    System.out.printf(
        "[DIFC] attestationLoad requester=%s path=%s exists=%s%n",
        requesterPrincipal, path, java.nio.file.Files.exists(path));
    final AttestedProcessingPolicy attestation = AttestedProcessingPolicy.readFromFile(path);
    final DifcTagPolicyVerifier.VerificationResult crypto =
        verifyAttestation(requesterPrincipal, attestation);
    if (!crypto.allowed()) {
      System.out.printf(
          "[DIFC] attestationLoad requester=%s phase=denied reason=\"%s\"%n",
          requesterPrincipal, crypto.reason());
      return new LoadedAttestedPolicy(null, crypto);
    }
    final byte[] payload = Base64.getDecoder().decode(attestation.getSignedPayloadBase64());
    final AppProcessingPolicy policy =
        AppProcessingPolicy.readFromCanonicalJson(new String(payload, StandardCharsets.UTF_8));
    System.out.printf(
        "[DIFC] attestationLoad requester=%s phase=ok policy-principal=%s%n",
        requesterPrincipal, policy != null ? policy.getPrincipal() : "null");
    return new LoadedAttestedPolicy(policy, crypto);
  }

  public static LoadedAttestedPolicy loadAndVerifyAttestationFromBytes(
      final String requesterPrincipal,
      final byte[] policyBytes) throws IOException {
    if (policyBytes == null || policyBytes.length == 0) {
      return new LoadedAttestedPolicy(
          null,
          DifcTagPolicyVerifier.VerificationResult.deny(
              "no attested processing-policy in GRANT_CAP request"));
    }
    final AttestedProcessingPolicy attestation = AttestedProcessingPolicy.readFromBytes(policyBytes);
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
    final String configured =
        firstNonBlank(System.getProperty(TRUSTED_CA_PATH_PROP), System.getenv(TRUSTED_CA_PATH_ENV));
    final String fallback =
        firstNonBlank(System.getProperty(FALLBACK_CA_PATH_PROP), System.getenv(FALLBACK_CA_PATH_ENV));
    final String path = firstNonBlank(configured, fallback);
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

  private static String firstNonBlank(final String... values) {
    if (values == null) {
      return null;
    }
    for (final String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  public record LoadedAttestedPolicy(
      AppProcessingPolicy policy,
      DifcTagPolicyVerifier.VerificationResult attestationResult) {}
}
