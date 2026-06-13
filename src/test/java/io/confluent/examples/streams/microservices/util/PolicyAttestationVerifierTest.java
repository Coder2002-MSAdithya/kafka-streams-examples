package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PolicyAttestationVerifierTest {

  private static Path trustedCaPath;
  private static PrivateKey signingKey;
  private static String certificatePem;

  @BeforeAll
  static void loadFixtureMaterial() throws Exception {
    final Path base = Path.of("src/test/resources/policy-attestation");
    trustedCaPath = base.resolve("test-trusted-ca.pem");
    signingKey = loadPrivateKey(Files.readString(base.resolve("test-signing-key.pem")));
    certificatePem = Files.readString(base.resolve("test-signing-cert.pem"));
    System.setProperty(PolicyAttestationVerifier.TRUSTED_CA_PATH_PROP, trustedCaPath.toString());
  }

  @Test
  void verifiesSignedPolicyAndSemanticRules(@TempDir final Path registryDir) throws Exception {
    writeAttestedPolicy(registryDir, "fraud-svc", canonicalFraudPolicy(), false);

    System.setProperty(DifcTagPolicyVerifier.POLICY_REGISTRY_DIR_PROP, registryDir.toString());
    final DifcTagPolicyVerifier.VerificationResult result =
        DifcTagPolicyVerifier.verifyAttestationAndCanRemoveOnTag(
            "fraud-svc", DifcGrantPolicy.PRINCIPAL_ORDERS, DifcGrantPolicy.TAG_ORDER);
    assertTrue(result.allowed(), result.reason());
  }

  @Test
  void rejectsTamperedSignature(@TempDir final Path registryDir) throws Exception {
    writeAttestedPolicy(registryDir, "fraud-svc", canonicalFraudPolicy(), true);

    System.setProperty(DifcTagPolicyVerifier.POLICY_REGISTRY_DIR_PROP, registryDir.toString());
    final DifcTagPolicyVerifier.VerificationResult result =
        PolicyAttestationVerifier.verifyAttestation(
            "fraud-svc",
            AttestedProcessingPolicy.readFromFile(
                registryDir.resolve("fraud-svc/processing-policy.json")));
    assertFalse(result.allowed());
  }

  private void writeAttestedPolicy(
      final Path registryDir,
      final String principal,
      final String canonicalPolicy,
      final boolean tamperSignature) throws Exception {
    final Path principalDir = registryDir.resolve(principal);
    Files.createDirectories(principalDir);

    final byte[] payload = canonicalPolicy.getBytes(StandardCharsets.UTF_8);
    final Signature signature = Signature.getInstance("SHA256withECDSA");
    signature.initSign(signingKey);
    signature.update(payload);
    String signatureBase64 = Base64.getEncoder().encodeToString(signature.sign());
    if (tamperSignature) {
      signatureBase64 = signatureBase64.substring(0, signatureBase64.length() - 4) + "AAAA";
    }

    final String envelope = """
        {
          "format": "difc-processing-policy-attestation-v1",
          "signatureAlgorithm": "SHA256withECDSA",
          "signedPayloadBase64": "%s",
          "signatureBase64": "%s",
          "certificatePem": %s
        }
        """.formatted(
        Base64.getEncoder().encodeToString(payload),
        signatureBase64,
        jsonString(certificatePem));

    Files.writeString(principalDir.resolve("processing-policy.json"), envelope);
  }

  private static String canonicalFraudPolicy() throws Exception {
    return new ObjectMapper()
        .writeValueAsString(
            AgentPolicyTestSupport.agentEnriched(
                """
                {
                  "version": 2,
                  "generatedAt": "2026-01-01T00:00:00Z",
                  "topologyDigest": "abc",
                  "principal": "fraud-svc",
                  "service": "FraudService",
                  "components": ["kafka-streams"],
                  "sources": ["orders"],
                  "aggregations": ["merge"],
                  "sinks": [
                    {
                      "topic": "order-validations",
                      "declassifyTags": ["order"],
                      "addTags": ["fraud"]
                    }
                  ],
                  "egressPaths": [
                    {
                      "topic": "order-validations",
                      "ingressTopics": ["orders"],
                      "operators": ["filter", "mapValues", "merge"],
                      "declassifyTags": ["order"],
                      "addTags": ["fraud"]
                    }
                  ],
                  "graph": {
                    "nodes": [
                      {"id":"topic_orders","kind":"topic","topic":"orders"},
                      {"id":"op_filter","kind":"operator","label":"filter()"},
                      {"id":"op_map","kind":"operator","label":"mapValues()"},
                      {"id":"op_merge","kind":"operator","label":"merge()"},
                      {"id":"topic_order_validations","kind":"topic","topic":"order-validations"}
                    ],
                    "edges": [
                      {"from":"topic_orders","to":"op_filter","label":"source"},
                      {"from":"op_filter","to":"op_map","label":"output"},
                      {"from":"op_map","to":"op_merge","label":"input"},
                      {"from":"op_merge","to":"topic_order_validations","label":"writes"}
                    ]
                  }
                }
                """));
  }

  private static String jsonString(final String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
  }

  private static PrivateKey loadPrivateKey(final String pem) throws Exception {
    final String body = pem
        .replace("-----BEGIN PRIVATE KEY-----", "")
        .replace("-----END PRIVATE KEY-----", "")
        .replaceAll("\\s", "");
    return KeyFactory.getInstance("EC")
        .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(body)));
  }
}
