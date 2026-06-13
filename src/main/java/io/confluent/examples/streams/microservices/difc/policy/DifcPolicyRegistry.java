package io.confluent.examples.streams.microservices.difc.policy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Publishes agent-captured (or hand-authored) processing policies for grant-time verification.
 */
public final class DifcPolicyRegistry {

    private DifcPolicyRegistry() {
    }

    public static void publishFromAgentOrManifest(
            final String principal,
            final String applicationId,
            final String stateDir,
            final ProcessingPolicyDocument manifestFallback) {
        final Path agentPath = DifcPolicyPaths.agentPolicyPath(stateDir, principal);
        final Path published = DifcPolicyPaths.publishedPolicyForPrincipal(principal);
        ProcessingPolicyDocument policy = waitForAgentPolicy(agentPath, 30);
        if (policy == null && manifestFallback != null) {
            policy = manifestFallback;
            policy.setPrincipal(principal);
            policy.setApplicationId(applicationId);
            try {
                ProcessingPolicyDocument.write(agentPath, policy);
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to write manifest policy to " + agentPath, e);
            }
        } else if (policy != null && manifestFallback != null) {
            mergeManifestFallback(policy, manifestFallback);
        }
        if (policy == null) {
            System.out.printf(
                    "[DIFC][policy] WARN no processing policy found for principal=%s (expected %s)%n",
                    principal, agentPath);
            return;
        }
        if (policy.getPrincipal() == null || policy.getPrincipal().isBlank()) {
            policy.setPrincipal(principal);
        }
        if (policy.getApplicationId() == null || policy.getApplicationId().isBlank()) {
            policy.setApplicationId(applicationId);
        }
        try {
            Files.createDirectories(published.getParent());
            ProcessingPolicyDocument.write(published, policy);
            System.out.printf(
                    "[DIFC][policy] published principal=%s app=%s sinks=%d tagFlows=%d digest=%s -> %s%n",
                    principal,
                    policy.getApplicationId(),
                    policy.getSinkBindings().size(),
                    policy.getTagFlows().size(),
                    policy.getTopologyDigest(),
                    published);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to publish policy for " + principal, e);
        }
    }

    public static ProcessingPolicyDocument loadPublished(final String principal) {
        try {
            return ProcessingPolicyDocument.read(DifcPolicyPaths.publishedPolicyForPrincipal(principal));
        } catch (final IOException e) {
            System.out.printf("[DIFC][policy] WARN failed to read policy for %s: %s%n", principal, e.getMessage());
            return null;
        }
    }

    private static void mergeManifestFallback(
            final ProcessingPolicyDocument policy,
            final ProcessingPolicyDocument manifestFallback) {
        if (policy.getSinkBindings().isEmpty() && !manifestFallback.getSinkBindings().isEmpty()) {
            policy.setSinkBindings(manifestFallback.getSinkBindings());
        }
        for (final TagFlowPolicy manifestFlow : manifestFallback.getTagFlows()) {
            final TagFlowPolicy existing = policy.getTagFlows().stream()
                    .filter(flow -> manifestFlow.getTag().equals(flow.getTag()))
                    .findFirst()
                    .orElse(null);
            if (existing == null) {
                policy.getTagFlows().add(manifestFlow);
                continue;
            }
            if (existing.getIngressTopics().isEmpty()) {
                existing.setIngressTopics(manifestFlow.getIngressTopics());
            }
            if (existing.getEgressTopics().isEmpty()) {
                existing.setEgressTopics(manifestFlow.getEgressTopics());
            }
            if (!existing.isRemovedOnEgress() && manifestFlow.isRemovedOnEgress()) {
                existing.setRemovedOnEgress(true);
            }
            if (!existing.hasAggregator()) {
                existing.setAggregators(manifestFlow.getAggregators());
            }
        }
    }

    private static ProcessingPolicyDocument waitForAgentPolicy(final Path agentPath, final int seconds) {
        for (int attempt = 1; attempt <= seconds; attempt++) {
            try {
                if (Files.isRegularFile(agentPath)) {
                    return ProcessingPolicyDocument.read(agentPath);
                }
            } catch (final IOException ignored) {
            }
            try {
                TimeUnit.SECONDS.sleep(1L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }
}
