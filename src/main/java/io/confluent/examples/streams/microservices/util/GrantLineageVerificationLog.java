package io.confluent.examples.streams.microservices.util;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Grantor-side audit lines for expression-lineage vs fallback sanitization checks. */
public final class GrantLineageVerificationLog {

  private GrantLineageVerificationLog() {}

  public static void republicationFastPath(
      final String ownedTag,
      final String grantorPrincipal,
      final Set<String> consumedFromGrantorPublish) {
    System.out.printf(
        "[DIFC] grantLineageVerify phase=republication-fast-path tag=%s grantor=%s "
            + "consumedFromGrantorPublish=%s note=per-path expression-lineage skipped "
            + "(consumer reads grantor publish topic directly)%n",
        ownedTag,
        grantorPrincipal,
        consumedFromGrantorPublish);
  }

  public static void verifyingPath(
      final String ownedTag,
      final String inputTopic,
      final String outputTopic,
      final int egressLineageCount) {
    System.out.printf(
        "[DIFC] grantLineageVerify phase=begin tag=%s path=%s→%s egressLineageFields=%d%n",
        ownedTag,
        inputTopic,
        outputTopic,
        egressLineageCount);
  }

  public static void verificationPath(
      final String ownedTag,
      final String inputTopic,
      final String outputTopic,
      final String pathKind,
      final boolean sanitized) {
    System.out.printf(
        "[DIFC] grantLineageVerify phase=path tag=%s path=%s→%s method=%s sanitized=%s%n",
        ownedTag,
        inputTopic,
        outputTopic,
        pathKind,
        sanitized);
  }

  public static void expressionLineageDecision(
      final String ownedTag,
      final String inputTopic,
      final String outputTopic,
      final Set<String> taggedSensitive,
      final Map<String, FieldLineage> egressLineages,
      final Set<String> leaked,
      final boolean sanitized) {
    System.out.printf(
        "[DIFC] grantLineageVerify phase=expression-lineage tag=%s path=%s→%s "
            + "sensitiveOnInput=%s egressLineage=%s leaked=%s sanitized=%s%n",
        ownedTag,
        inputTopic,
        outputTopic,
        taggedSensitive,
        summarizeLineages(egressLineages),
        leaked,
        sanitized);
  }

  public static void taintDecision(
      final String ownedTag,
      final String inputTopic,
      final String outputTopic,
      final Set<String> taggedSensitive,
      final java.util.List<AppProcessingPolicy.SourceFieldTaintStatus> sourceFieldTaint,
      final Set<String> unsanitized,
      final boolean sanitized) {
    System.out.printf(
        "[DIFC] grantLineageVerify phase=taint-report tag=%s path=%s→%s "
            + "sensitiveOnInput=%s sourceFieldTaint=%s unsanitized=%s sanitized=%s%n",
        ownedTag,
        inputTopic,
        outputTopic,
        taggedSensitive,
        summarizeTaint(sourceFieldTaint),
        unsanitized,
        sanitized);
  }

  private static String summarizeLineages(final Map<String, FieldLineage> egressLineages) {
    if (egressLineages == null || egressLineages.isEmpty()) {
      return "[]";
    }
    return egressLineages.entrySet().stream()
        .map(
            e -> {
              final FieldLineage lineage = e.getValue();
              if (lineage == null) {
                return e.getKey() + "=?";
              }
              final String expr = lineage.getExpression();
              final String kind =
                  lineage.sanitizationKindEnum() == null
                      ? "?"
                      : lineage.sanitizationKindEnum().name();
              final String sources = lineage.sourceFieldSet().toString();
              if (expr == null || expr.isEmpty()) {
                return e.getKey() + "(" + kind + ", src=" + sources + ")";
              }
              return e.getKey() + "=" + expr + "(" + kind + ", src=" + sources + ")";
            })
        .collect(Collectors.joining("; ", "[", "]"));
  }

  private static String summarizeTaint(
      final java.util.List<AppProcessingPolicy.SourceFieldTaintStatus> sourceFieldTaint) {
    if (sourceFieldTaint == null || sourceFieldTaint.isEmpty()) {
      return "[]";
    }
    return sourceFieldTaint.stream()
        .map(
            status ->
                status.getSourceField()
                    + "="
                    + status.getStatus()
                    + "(sink="
                    + status.getSinkFields()
                    + ")")
        .collect(Collectors.joining("; ", "[", "]"));
  }
}
