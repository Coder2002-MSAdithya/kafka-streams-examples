package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Grantor-side {@code CAN_REMOVE} verification for owned tag {@code X}:
 * <ol>
 *   <li>Topics where grantor publishes tag {@code X}</li>
 *   <li>Vacuous allow if requester consumes none of them</li>
 *   <li>Otherwise every output-topic relation {@code input → egress} that involves a consumed
 *       grantor topic must have a relational-algebra plan (extracted from the processing graph)
 *       that sanitizes grantor-defined sensitive fields on that tagged input</li>
 * </ol>
 */
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

    final Set<String> grantorPublishTopics =
        new LinkedHashSet<>(GrantorTagTopology.grantorPublishTopicsForTag(grantorPrincipal, ownedTag));

    final Set<String> consumedGrantorTopics =
        ProcessingPolicyGraphHelper.consumedTopics(rawPolicy, grantorPublishTopics);
    if (consumedGrantorTopics.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.allow(
          "vacuous: requester does not consume grantor topics publishing tag " + ownedTag);
    }

    if (rawPolicy.getRelationalAlgebraAnalysis() == null
        || rawPolicy.getRelationalAlgebraAnalysis().getProcessingPaths().isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "signed policy missing agent-computed relationalAlgebraAnalysis");
    }

    final List<AppProcessingPolicy.EgressPath> processingPaths =
        ProcessingPolicyGraphHelper.egressPathsProcessingTopics(rawPolicy, consumedGrantorTopics);
    if (processingPaths.isEmpty()) {
      return DifcTagPolicyVerifier.VerificationResult.deny(
          "requester consumes "
              + consumedGrantorTopics
              + " but no egress path processes grantor-tagged data for tag "
              + ownedTag);
    }

    for (final String consumed : consumedGrantorTopics) {
      for (final AppProcessingPolicy.EgressPath egress : processingPaths) {
        if (!ProcessingPolicyGraphHelper.ingressTopicsForEgressPath(rawPolicy, egress)
            .contains(consumed)) {
          continue;
        }
        if (!GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            ownedTag, consumed, egress.getTopic(), rawPolicy)) {
          final AppProcessingPolicy.ProcessingPathAnalysis failedPath =
              GrantRelationalAlgebraVerifier.pathAnalysisFor(rawPolicy, consumed, egress.getTopic());
          return DifcTagPolicyVerifier.VerificationResult.deny(
              GrantRelationalAlgebraVerifier.denialReason(
                  ownedTag, consumed, egress.getTopic(), failedPath));
        }
      }
    }

    return DifcTagPolicyVerifier.VerificationResult.allow("policy verified");
  }
}
