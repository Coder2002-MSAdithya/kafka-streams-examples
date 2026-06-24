package io.confluent.examples.streams.microservices.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Agent-signed processing policy envelope ({@code processing-policy.json}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AttestedProcessingPolicy {

  public static final String SIGNED_FORMAT = "difc-processing-policy-attestation-v1";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private String format;
  private String signatureAlgorithm;
  private String signedPayloadBase64;
  private String signatureBase64;
  private String certificatePem;

  public String getFormat() {
    return format;
  }

  public String getSignatureAlgorithm() {
    return signatureAlgorithm;
  }

  public String getSignedPayloadBase64() {
    return signedPayloadBase64;
  }

  public String getSignatureBase64() {
    return signatureBase64;
  }

  public String getCertificatePem() {
    return certificatePem;
  }

  public static AttestedProcessingPolicy readFromFile(final Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    if (!Files.isRegularFile(path)) {
      return null;
    }
    return MAPPER.readValue(Files.readString(path), AttestedProcessingPolicy.class);
  }

  public static AttestedProcessingPolicy readFromBytes(final byte[] bytes) throws IOException {
    Objects.requireNonNull(bytes, "bytes");
    if (bytes.length == 0) {
      return null;
    }
    return MAPPER.readValue(bytes, AttestedProcessingPolicy.class);
  }
}
