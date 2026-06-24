package io.confluent.examples.streams.microservices.util;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Grantor-side {@code CAN_REMOVE} verification using tag-carrier topics. */
public final class ProcessingPolicyGraphAnalyzer {

  private ProcessingPolicyGraphAnalyzer() {
  }

  public static DifcTagPolicyVerifier.VerificationResult verifyCanRemoveOnTag(
      final String ownedTag,
      final String grantorPrincipal,
      final AppProcessingPolicy rawPolicy) {
    final String requester = rawPolicy != null ? rawPolicy.getPrincipal() : "unknown";
    System.out.printf(
        "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=start%n",
        ownedTag, grantorPrincipal, requester);

    if (rawPolicy == null) {
      final String reason = "no processing-policy.json for requester";
      System.out.printf(
          "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=deny reason=\"%s\"%n",
          ownedTag, grantorPrincipal, requester, reason);
      return DifcTagPolicyVerifier.VerificationResult.deny(reason);
    }
    if (!GrantorTagTopology.ownsTag(grantorPrincipal, ownedTag)) {
      final String reason = "grantor " + grantorPrincipal + " does not own tag " + ownedTag;
      System.out.printf(
          "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=deny reason=\"%s\"%n",
          ownedTag, grantorPrincipal, requester, reason);
      return DifcTagPolicyVerifier.VerificationResult.deny(reason);
    }

    final Set<String> tagCarrierTopics =
        new LinkedHashSet<>(GrantorTagTopology.tagCarrierTopics(ownedTag));
    System.out.printf(
        "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=carrier-topics topics=%s%n",
        ownedTag, grantorPrincipal, requester, tagCarrierTopics);

    final Set<String> consumedTaggedTopics =
        ProcessingPolicyGraphHelper.consumedTopics(rawPolicy, tagCarrierTopics);
    System.out.printf(
        "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=consumed-tagged topics=%s%n",
        ownedTag, grantorPrincipal, requester, consumedTaggedTopics);

    if (consumedTaggedTopics.isEmpty()) {
      final String reason = "vacuous: requester does not consume topics carrying tag " + ownedTag;
      System.out.printf(
          "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=allow reason=\"%s\"%n",
          ownedTag, grantorPrincipal, requester, reason);
      System.out.printf(
          "[DIFC][RA-SUMMARY] tag=%s requester=%s verdict=ALLOW"
              + " rationale=\"%s does not consume any topic that carries tag '%s'"
              + " (carrier topics: %s), so there are no sensitive fields to leak;"
              + " CAN_REMOVE is trivially safe (vacuous grant).\"%n",
          ownedTag, requester, requester, ownedTag, tagCarrierTopics);
      return DifcTagPolicyVerifier.VerificationResult.allow(reason);
    }

    if (rawPolicy.getRelationalAlgebraAnalysis() == null
        || rawPolicy.getRelationalAlgebraAnalysis().getProcessingPaths().isEmpty()) {
      final String reason = "signed policy missing agent-computed relationalAlgebraAnalysis";
      System.out.printf(
          "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=deny reason=\"%s\"%n",
          ownedTag, grantorPrincipal, requester, reason);
      System.out.printf(
          "[DIFC][RA-SUMMARY] tag=%s requester=%s verdict=DENY"
              + " rationale=\"The signed processing-policy.json exported by the policy agent"
              + " for %s contains no relationalAlgebraAnalysis section. Without a verified"
              + " topology description the grantor cannot determine whether sensitive fields"
              + " of tag '%s' are sanitized, so CAN_REMOVE is denied.\"%n",
          ownedTag, requester, requester, ownedTag);
      return DifcTagPolicyVerifier.VerificationResult.deny(reason);
    }

    final List<AppProcessingPolicy.EgressPath> processingPaths =
        ProcessingPolicyGraphHelper.egressPathsProcessingTopics(rawPolicy, consumedTaggedTopics);
    System.out.printf(
        "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=egress-paths count=%d paths=%s%n",
        ownedTag, grantorPrincipal, requester, processingPaths.size(),
        processingPaths.stream().map(AppProcessingPolicy.EgressPath::getTopic)
            .collect(java.util.stream.Collectors.toList()));

    if (processingPaths.isEmpty()) {
      final String reason = "requester consumes "
          + consumedTaggedTopics
          + " carrying tag "
          + ownedTag
          + " but no egress path processes that input";
      System.out.printf(
          "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=deny reason=\"%s\"%n",
          ownedTag, grantorPrincipal, requester, reason);
      return DifcTagPolicyVerifier.VerificationResult.deny(reason);
    }

    for (final String consumed : consumedTaggedTopics) {
      for (final AppProcessingPolicy.EgressPath egress : processingPaths) {
        if (!ProcessingPolicyGraphHelper.ingressTopicsForEgressPath(rawPolicy, egress)
            .contains(consumed)) {
          continue;
        }
        final boolean sanitized = GrantRelationalAlgebraVerifier.relationSanitizesTaggedInput(
            ownedTag, consumed, egress.getTopic(), rawPolicy);
        System.out.printf(
            "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=path-sanitize"
                + " consumed=%s egress=%s sanitized=%s%n",
            ownedTag, grantorPrincipal, requester, consumed, egress.getTopic(), sanitized);
        if (!sanitized) {
          final AppProcessingPolicy.ProcessingPathAnalysis failedPath =
              GrantRelationalAlgebraVerifier.pathAnalysisFor(rawPolicy, consumed, egress.getTopic());
          final String reason;
          if (failedPath == null) {
            reason = "no relational-algebra plan for "
                + consumed
                + " → "
                + egress.getTopic()
                + " (tag "
                + ownedTag
                + ")";
          } else {
            reason = GrantRelationalAlgebraVerifier.denialReason(
                ownedTag, consumed, egress.getTopic(), failedPath);
          }
          System.out.printf(
              "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=deny reason=\"%s\"%n",
              ownedTag, grantorPrincipal, requester, reason);
          System.out.printf(
              "[DIFC][RA-SUMMARY] tag=%s requester=%s path=%s→%s verdict=DENY"
                  + " rationale=\"Sanitization check failed for the data-flow path %s → %s."
                  + " %s The grantor (%s) requires that all fields sensitive under tag '%s'"
                  + " be absent from the output; this output relation does not satisfy"
                  + " that requirement, so CAN_REMOVE on tag '%s' is denied.\"%n",
              ownedTag, requester, consumed, egress.getTopic(),
              consumed, egress.getTopic(), reason, grantorPrincipal, ownedTag, ownedTag);
          return DifcTagPolicyVerifier.VerificationResult.deny(reason);
        }
      }
    }

    final Set<String> grantorPublishTopics =
        new LinkedHashSet<>(GrantorTagTopology.grantorPublishTopicsForTag(grantorPrincipal, ownedTag));
    final Set<String> consumedFromGrantorPublish = new LinkedHashSet<>(consumedTaggedTopics);
    consumedFromGrantorPublish.retainAll(grantorPublishTopics);
    if (consumedFromGrantorPublish.isEmpty()) {
      final String reason = "policy verified: requester consumes "
          + consumedTaggedTopics
          + " carrying tag "
          + ownedTag
          + " (republication; grantor publishes tag only on "
          + grantorPublishTopics
          + "); RA sanitizes sensitive fields";
      System.out.printf(
          "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=allow reason=\"%s\"%n",
          ownedTag, grantorPrincipal, requester, reason);
      System.out.printf(
          "[DIFC][RA-SUMMARY] tag=%s requester=%s verdict=ALLOW"
              + " rationale=\"%s consumes %s (a republication of tag '%s', not the grantor's"
              + " primary publish topic %s). RA/taint verification confirmed that all"
              + " grantor-sensitive fields are projected away on every path; CAN_REMOVE"
              + " is safe to grant.\"%n",
          ownedTag, requester, requester, consumedTaggedTopics, ownedTag, grantorPublishTopics);
      return DifcTagPolicyVerifier.VerificationResult.allow(reason);
    }
    GrantLineageVerificationLog.republicationFastPath(
        ownedTag, grantorPrincipal, consumedFromGrantorPublish);
    final String reason = "policy verified: requester consumes grantor publish topic(s) "
        + consumedFromGrantorPublish
        + " carrying tag "
        + ownedTag
        + "; RA sanitizes sensitive fields";
    System.out.printf(
        "[DIFC] canRemoveVerify tag=%s grantor=%s requester=%s phase=allow reason=\"%s\"%n",
        ownedTag, grantorPrincipal, requester, reason);
    System.out.printf(
        "[DIFC][RA-SUMMARY] tag=%s requester=%s verdict=ALLOW"
            + " rationale=\"%s consumes grantor publish topic(s) %s carrying tag '%s'."
            + " RA/taint verification confirmed that all grantor-sensitive fields are"
            + " projected away before the output topic; the output relation does not"
            + " retain any sensitive field of tag '%s'. CAN_REMOVE is safe to grant.\"%n",
        ownedTag, requester, requester, consumedFromGrantorPublish, ownedTag, ownedTag);
    return DifcTagPolicyVerifier.VerificationResult.allow(reason);
  }
}
