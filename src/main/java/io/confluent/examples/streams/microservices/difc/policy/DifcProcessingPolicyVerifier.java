package io.confluent.examples.streams.microservices.difc.policy;

import io.confluent.examples.streams.microservices.util.DifcGrantPolicy;

import org.apache.kafka.clients.Capability;

import java.util.List;
import java.util.Set;

/**
 * Verifies that a requester's published processing policy properly aggregates data tagged with the
 * grantor-owned tag across all grantor egress topics.
 */
public final class DifcProcessingPolicyVerifier {

    private DifcProcessingPolicyVerifier() {
    }

    public static boolean verifyGrant(
            final String grantorPrincipal,
            final String ownedTag,
            final String requesterPrincipal,
            final Capability capability) {
        final Set<String> grantorEgress = GrantorTagEgressRegistry.egressTopicsForTag(grantorPrincipal, ownedTag);
        if (grantorEgress.isEmpty()) {
            System.out.printf(
                    "[DIFC][policy] deny grantor=%s tag=%s requester=%s reason=no grantor egress registry%n",
                    grantorPrincipal, ownedTag, requesterPrincipal);
            return false;
        }

        final ProcessingPolicyDocument requesterPolicy = DifcPolicyRegistry.loadPublished(requesterPrincipal);
        if (requesterPolicy == null) {
            System.out.printf(
                    "[DIFC][policy] deny grantor=%s tag=%s requester=%s reason=no published policy%n",
                    grantorPrincipal, ownedTag, requesterPrincipal);
            return false;
        }

        final List<TagFlowPolicy> flows = requesterPolicy.flowsForTag(ownedTag);
        if (flows.isEmpty()) {
            System.out.printf(
                    "[DIFC][policy] deny grantor=%s tag=%s requester=%s reason=policy has no flow for tag%n",
                    grantorPrincipal, ownedTag, requesterPrincipal);
            return false;
        }

        final TagFlowPolicy matchingFlow = flows.stream()
                .filter(flow -> flow.ingestsFromAny(grantorEgress))
                .findFirst()
                .orElse(null);
        if (matchingFlow == null) {
            System.out.printf(
                    "[DIFC][policy] deny grantor=%s tag=%s requester=%s reason=does not ingest grantor egress %s%n",
                    grantorPrincipal, ownedTag, requesterPrincipal, grantorEgress);
            return false;
        }

        if (DifcGrantPolicy.PRINCIPAL_VALIDATIONS_AGG.equals(requesterPrincipal)) {
            if (!matchingFlow.hasAggregator() || !matchingFlow.hasJoinAggregator()) {
                System.out.printf(
                        "[DIFC][policy] deny grantor=%s tag=%s requester=%s reason=aggregator must join+aggregate tagged inputs%n",
                        grantorPrincipal, ownedTag, requesterPrincipal);
                return false;
            }
        } else if (requiresAggregation(requesterPrincipal, ownedTag) && !matchingFlow.hasAggregator()) {
            System.out.printf(
                    "[DIFC][policy] deny grantor=%s tag=%s requester=%s reason=validator must aggregate grantor-tagged ingress%n",
                    grantorPrincipal, ownedTag, requesterPrincipal);
            return false;
        }

        if (!matchingFlow.getEgressTopics().isEmpty()) {
            System.out.printf(
                    "[DIFC][policy] allow grantor=%s tag=%s requester=%s ingress=%s egress=%s aggregators=%s%n",
                    grantorPrincipal,
                    ownedTag,
                    requesterPrincipal,
                    matchingFlow.getIngressTopics(),
                    matchingFlow.getEgressTopics(),
                    matchingFlow.getAggregators());
        } else {
            System.out.printf(
                    "[DIFC][policy] allow grantor=%s tag=%s requester=%s ingress=%s (consume-only)%n",
                    grantorPrincipal,
                    ownedTag,
                    requesterPrincipal,
                    matchingFlow.getIngressTopics());
        }
        return true;
    }

    private static boolean requiresAggregation(final String requesterPrincipal, final String ownedTag) {
        if (!DifcGrantPolicy.TAG_ORDER.equals(ownedTag)) {
            return false;
        }
        return DifcGrantPolicy.PRINCIPAL_FRAUD.equals(requesterPrincipal)
                || DifcGrantPolicy.PRINCIPAL_INVENTORY.equals(requesterPrincipal);
    }
}
