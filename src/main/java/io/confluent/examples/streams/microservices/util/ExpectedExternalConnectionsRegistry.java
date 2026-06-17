package io.confluent.examples.streams.microservices.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Expected non-Kafka endpoints per principal (Schema Registry HTTP, etc.). */
public final class ExpectedExternalConnectionsRegistry {

  private static final Set<String> SCHEMA_REGISTRY =
      Set.of("127.0.0.1:8081", "localhost:8081");

  private static final Map<String, Set<String>> DEFAULTS = buildDefaults();

  private ExpectedExternalConnectionsRegistry() {
  }

  public static Set<String> defaultsForPrincipal(final String principal) {
    if (principal == null) {
      return Set.of();
    }
    return DEFAULTS.getOrDefault(normalizePrincipal(principal), Set.of());
  }

  private static String normalizePrincipal(final String principal) {
    if (principal == null) {
      return "";
    }
    if (principal.endsWith("-consumer")) {
      return principal.substring(0, principal.length() - "-consumer".length());
    }
    if (principal.endsWith("-producer")) {
      return principal.substring(0, principal.length() - "-producer".length());
    }
    return principal;
  }

  private static Map<String, Set<String>> buildDefaults() {
    final Map<String, Set<String>> defaults = new LinkedHashMap<>();
    for (final String principal :
        new String[] {
          DifcGrantPolicy.PRINCIPAL_ORDERS,
          DifcGrantPolicy.PRINCIPAL_FRAUD,
          DifcGrantPolicy.PRINCIPAL_INVENTORY,
          DifcGrantPolicy.PRINCIPAL_ORDER_DETAILS,
          DifcGrantPolicy.PRINCIPAL_EMAIL,
          DifcGrantPolicy.PRINCIPAL_VALIDATIONS_AGG
        }) {
      defaults.put(principal, SCHEMA_REGISTRY);
    }
    return Collections.unmodifiableMap(defaults);
  }
}
