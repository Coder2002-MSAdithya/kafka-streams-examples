package io.confluent.examples.streams.microservices.difc.policy;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class DifcPolicyPaths {

    private DifcPolicyPaths() {
    }

    public static Path sharedPolicyDir() {
        final String registry = System.getProperty("policy.registry.dir");
        if (registry != null && !registry.isBlank()) {
            return Paths.get(registry).toAbsolutePath();
        }
        final String configured = System.getProperty("difc.policy.dir");
        if (configured != null && !configured.isBlank()) {
            return Paths.get(configured).toAbsolutePath();
        }
        return Paths.get("/tmp/ms-difc-policies").toAbsolutePath();
    }

    public static Path publishedPolicyForPrincipal(final String principal) {
        return sharedPolicyDir().resolve(principal + ".json");
    }

    public static Path agentPolicyPath(final String stateDir, final String principal) {
        final String jsonPath = System.getProperty("policy.dsl.json.path");
        if (jsonPath != null && !jsonPath.isBlank()) {
            return Paths.get(jsonPath).toAbsolutePath();
        }
        final String appPolicyPath = System.getProperty("policy.app.policy.path");
        if (appPolicyPath != null && !appPolicyPath.isBlank()) {
            return Paths.get(appPolicyPath).toAbsolutePath();
        }
        return Paths.get(stateDir, "policy", principal, "processing-policy.json").toAbsolutePath();
    }
}
