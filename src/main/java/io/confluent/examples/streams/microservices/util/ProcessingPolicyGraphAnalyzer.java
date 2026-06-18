package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Grantor-side {@code CAN_REMOVE} verification using tag-carrier topics. */
public final class ProcessingPolicyGraphAnalyzer {

  private ProcessingPolicyGraphAnalyzer() {
  }

  public static DifcTagPolicyVerifier.VerificationResult verifyCanRemoveOnTag(
      final String ownedTag,
      final String grantorPrincipal,
      final AppProcessingPolicy rawPolicy) {
    if (rawPolicy == null) {
      return DifcTagPolicyVerifier.VerificationResult.deny("no processing-policy.json for requester");
    }
    if (!GrantorTagTopology.ownsTag(grantorPrincipal, ownedTag)) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "grantor " + grantorPrincipal + " does not own tag " + ownedTag);
    }

    final Set<String> tagCarrierTopics =
        new LinkedHashSet<>(GrantorTagTopology.tagCarrierTopics(ownedTag));

    final Set<String> consumedTaggedTopics =
        ProcessingPolicyGraphHelper.consumedTopics(rawPolicy, tagCarrierTopics);
    if (consumedTaggedTopics.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.allow(
          "vacuous: requester does not consume topics carrying tag " + ownedTag);
    }

    if (rawPolicy.getRelationalAlgebraAnalysis() == null
        || rawPolicy.getRelationalAlgebraAnalysis().getProcessingPaths().isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "signed policy missing agent-computed relationalAlgebraAnalysis");
    }

    final List<AppProcessingPolicy.EgressPath> processingPaths =
        ProcessingPolicyGraphHelper.egressPathsProcessingTopics(rawPolicy, consumedTaggedTopics);
    if (processingPaths.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "requester consumes "
              + consumedTaggedTopics
              + " carrying tag "
              + ownedTag
              + " but no egress path processes that input");
    }

    for (final String consumed : consumedTaggedTopics) {
      for (final AppProcessingPolicy.EgressPath egress : processingPaths) {
        if (!ProcessingPolicyGraphHelper.ingressTopicsForEgressPath(rawPolicy, egress)
            .contains(consumed)) {
          continue;
        }
        if (!GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            ownedTag, consumed, egress.getTopic(), rawPolicy)) {
          final AppProcessingPolicy.ProcessingPathAnalysis failedPath =
              GrantRelationalAlgebraVerifier.pathAnalysisFor(rawPolicy, consumed, egress.getTopic());
          if (failedPath == null) {
            return DifcTagPolicyVerifier.VerificationResult.deny(
                "no relational-algebra plan for "
                    + consumed
                    + " → "
                    + egress.getTopic()
                    + " (tag "
                    + ownedTag
                    + ")");
          }
          return DifcTagPolicyVerifier.VerificationResult.deny(
              GrantRelationalAlgebraVerifier.denialReason(
                  ownedTag, consumed, egress.getTopic(), failedPath));
        }
      }
    }

    final Set<String> grantorPublishTopics =
        new LinkedHashSet<>(GrantorTagTopology.grantorPublishTopicsForTag(grantorPrincipal, ownedTag));
    final Set<String> consumedFromGrantorPublish = new LinkedHashSet<>(consumedTaggedTopics);
    consumedFromGrantorPublish.retainAll(grantorPublishTopics);
    if (consumedFromGrantorPublish.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.allow(
          "policy verified: requester consumes "
              + consumedTaggedTopics
              + " carrying tag "
              + ownedTag
              + " (republication; grantor publishes tag only on "
              + grantorPublishTopics
              + "); RA sanitizes sensitive fields");
    }
    GrantLineageVerificationLog.republicationFastPath(
        ownedTag, grantorPrincipal, consumedFromGrantorPublish);
    return DifcTagPolicyVerifier.VerificationResult.allow(
        "policy verified: requester consumes grantor publish topic(s) "
            + consumedFromGrantorPublish
            + " carrying tag "
            + ownedTag
            + "; RA sanitizes sensitive fields");
  }
}
