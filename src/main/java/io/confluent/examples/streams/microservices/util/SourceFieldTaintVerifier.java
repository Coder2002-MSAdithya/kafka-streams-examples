package io.confluent.examples.streams.microservices.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Evaluates grantor-sensitive fields against agent-emitted source-field taint reports. */
public final class SourceFieldTaintVerifier {

  private SourceFieldTaintVerifier() {}

  /**
   * @return nullable boolean; {@code null} means no taint report was available for the requested
   *     path/input topic.
   */
  public static Boolean scanSanitizesTaggedInput(
      final AppProcessingPolicy.ProcessingPathAnalysis path,
      final String ownedTag,
      final String inputTopic,
      final String outputTopic) {
    if (path == null || path.getSourceFieldTaint().isEmpty()) {
      return null;
    }
    final Set<String> sensitive = GrantorTagTopology.sensitiveFieldsForTaggedInput(ownedTag, inputTopic);
    if (sensitive.isEmpty()) {
      return true;
    }
    final List<AppProcessingPolicy.SourceFieldTaintStatus> relevant = new ArrayList<>();
    for (final AppProcessingPolicy.SourceFieldTaintStatus status : path.getSourceFieldTaint()) {
      if (status == null) {
        continue;
      }
      if (!inputTopic.equals(status.getSourceTopic())) {
        continue;
      }
      if (!sensitive.contains(status.getSourceField())) {
        continue;
      }
      relevant.add(status);
    }
    if (relevant.isEmpty()) {
      return null;
    }
    final Set<String> unsanitized = new LinkedHashSet<>();
    for (final AppProcessingPolicy.SourceFieldTaintStatus status : relevant) {
      if (!status.isSanitized()) {
        unsanitized.add(status.getSourceField() + "(" + status.getStatus() + ")");
      }
    }
    final boolean sanitized = unsanitized.isEmpty();
    GrantLineageVerificationLog.taintDecision(
        ownedTag, inputTopic, outputTopic, sensitive, relevant, unsanitized, sanitized);
    return sanitized;
  }
}
