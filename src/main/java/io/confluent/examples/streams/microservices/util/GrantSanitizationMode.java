package io.confluent.examples.streams.microservices.util;

import java.util.Locale;

/** Runtime mode selector for grant-time sanitization verification. */
public enum GrantSanitizationMode {
  LINEAGE,
  TAINT,
  HYBRID;

  private static final String PROP = "difc.grant.sanitization.mode";
  private static final String ENV = "DIFC_GRANT_SANITIZATION_MODE";

  public static GrantSanitizationMode current() {
    final String raw = readMode();
    if (raw == null || raw.isBlank()) {
      return LINEAGE;
    }
    return switch (raw.trim().toLowerCase(Locale.ROOT)) {
      case "lineage", "ra", "relational", "relational-algebra" -> LINEAGE;
      case "taint", "tainting" -> TAINT;
      case "hybrid", "both" -> HYBRID;
      default -> LINEAGE;
    };
  }

  private static String readMode() {
    final String prop = System.getProperty(PROP);
    if (prop != null && !prop.isBlank()) {
      return prop;
    }
    final String env = System.getenv(ENV);
    if (env != null && !env.isBlank()) {
      return env;
    }
    return null;
  }
}
