package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.microservices.util.AppProcessingPolicy;
import io.confluent.examples.streams.microservices.util.AttestedProcessingPolicy;
import io.confluent.examples.streams.microservices.util.ProcessingPolicyGraph;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;

/**
 * Prints agent-attested processing policy graphs and relational-algebra paths for demo services.
 */
public final class PrintProcessingPolicies {

  private static final List<String> WORKFLOW_PRINCIPALS = List.of(
      "orders-svc",
      "fraud-svc",
      "inventory-svc",
      "order-details-svc",
      "validations-agg-svc");

  private PrintProcessingPolicies() {
  }

  public static void main(final String[] args) throws Exception {
    final Options opts = new Options();
    opts.addOption(Option.builder("d")
        .longOpt("policy-dir").hasArg()
        .desc("Policy registry root (default: /tmp/kafka-streams-examples/policy)").build());
    opts.addOption(Option.builder("h")
        .longOpt("help").hasArg(false).desc("Show usage").build());

    final CommandLine cl = new DefaultParser().parse(opts, args);
    if (cl.hasOption("h")) {
      new HelpFormatter().printHelp("PrintProcessingPolicies", opts);
      return;
    }

    final Path policyDir = Paths.get(
        cl.getOptionValue("policy-dir", "/tmp/kafka-streams-examples/policy"));

    for (final String principal : WORKFLOW_PRINCIPALS) {
      printPrincipalPolicy(principal, policyDir.resolve(principal).resolve("processing-policy.json"));
    }
  }

  private static void printPrincipalPolicy(final String principal, final Path policyFile)
      throws Exception {
    System.out.println();
    System.out.println("=".repeat(78));
    System.out.printf("PRINCIPAL: %s%n", principal);
    System.out.println("=".repeat(78));

    if (!Files.isRegularFile(policyFile)) {
      System.out.printf("  (no policy file at %s)%n", policyFile);
      return;
    }

    final AttestedProcessingPolicy attestation = AttestedProcessingPolicy.readFromFile(policyFile);
    if (attestation == null || attestation.getSignedPayloadBase64() == null) {
      System.out.printf("  (empty or unreadable attestation at %s)%n", policyFile);
      return;
    }

    final byte[] payload = Base64.getDecoder().decode(attestation.getSignedPayloadBase64());
    final AppProcessingPolicy policy =
        AppProcessingPolicy.readFromCanonicalJson(new String(payload, StandardCharsets.UTF_8));
    if (policy == null) {
      System.out.println("  (signed payload did not decode to a processing policy)");
      return;
    }

    System.out.printf("Service:          %s%n", policy.getService());
    System.out.printf("Generated at:     %s%n", policy.getGeneratedAt());
    System.out.printf("Topology digest:  %s%n", policy.getTopologyDigest());
    System.out.printf("Components:       %s%n", policy.getComponents());
    System.out.printf("Sources:          %s%n", policy.getSources());
    System.out.printf("Aggregations:     %s%n", policy.getAggregations());

    printSinks(policy);
    printEgressPaths(policy);
    printGraph(policy.getGraph());
    printRelationalAlgebra(policy);
  }

  private static void printSinks(final AppProcessingPolicy policy) {
    System.out.println();
    System.out.println("Sinks:");
    if (policy.getSinks().isEmpty()) {
      System.out.println("  (none)");
      return;
    }
    for (final AppProcessingPolicy.SinkPolicy sink : policy.getSinks()) {
      System.out.printf(
          "  topic=%-22s declassify=%s add=%s%n",
          sink.getTopic(),
          sink.getDeclassifyTags(),
          sink.getAddTags());
    }
  }

  private static void printEgressPaths(final AppProcessingPolicy policy) {
    System.out.println();
    System.out.println("Egress paths:");
    if (policy.getEgressPaths().isEmpty()) {
      System.out.println("  (none)");
      return;
    }
    for (final AppProcessingPolicy.EgressPath path : policy.getEgressPaths()) {
      System.out.printf(
          "  -> %-22s ingress=%s operators=%s declassify=%s add=%s%n",
          path.getTopic(),
          path.getIngressTopics(),
          path.getOperators(),
          path.getDeclassifyTags(),
          path.getAddTags());
    }
  }

  private static void printGraph(final ProcessingPolicyGraph graph) {
    System.out.println();
    System.out.printf(
        "Policy graph (%d nodes, %d edges):%n",
        graph.getNodes().size(),
        graph.getEdges().size());

    graph.getNodes().stream()
        .sorted(Comparator.comparing(ProcessingPolicyGraph.GraphNode::getKind,
                Comparator.nullsLast(String::compareTo))
            .thenComparing(n -> n.getLabel() == null ? "" : n.getLabel()))
        .forEach(node -> {
          final String kind = node.getKind() == null ? "?" : node.getKind();
          final String label = node.getLabel() == null ? node.getId() : node.getLabel();
          if ("topic".equals(kind) && node.getTopic() != null) {
            System.out.printf("  [%s] %s (topic=%s)%n", kind, label, node.getTopic());
          } else if ("stateStore".equals(kind)) {
            System.out.printf("  [%s] %s%n", kind, shorten(node.getStoreName(), 72));
          } else if ("internalTopic".equals(kind)) {
            System.out.printf("  [%s] %s (topic=%s)%n", kind, label, node.getTopic());
          } else {
            System.out.printf("  [%s] %s%n", kind, shorten(label, 96));
          }
        });

    System.out.println();
    System.out.println("Graph edges:");
    graph.getEdges().stream()
        .sorted(Comparator.comparing(ProcessingPolicyGraph.GraphEdge::getFrom,
                Comparator.nullsLast(String::compareTo))
            .thenComparing(ProcessingPolicyGraph.GraphEdge::getTo,
                Comparator.nullsLast(String::compareTo)))
        .forEach(edge -> System.out.printf(
            "  %s --%s--> %s%n",
            edge.getFrom(),
            edge.getLabel() == null ? "" : edge.getLabel(),
            edge.getTo()));
  }

  private static void printRelationalAlgebra(final AppProcessingPolicy policy) {
    System.out.println();
    System.out.println("Relational algebra:");
    final AppProcessingPolicy.RelationalAlgebraAnalysis ra = policy.getRelationalAlgebraAnalysis();
    if (ra == null || ra.getProcessingPaths() == null || ra.getProcessingPaths().isEmpty()) {
      System.out.println("  (no relationalAlgebraAnalysis in signed policy)");
      return;
    }

    if (policy.getAggregationAnalysis() != null) {
      final AppProcessingPolicy.AggregationAnalysis agg = policy.getAggregationAnalysis();
      System.out.printf(
          "  aggregation summary: operators=%d joins=%d merges=%d consumedGrantorTopics=%s%n",
          agg.getTotalAggregationOperators(),
          agg.getJoinCount(),
          agg.getMergeCount(),
          agg.getConsumedGrantorTopics());
    }

    for (final AppProcessingPolicy.ProcessingPathAnalysis path : ra.getProcessingPaths()) {
      System.out.println();
      final String ingressSummary =
          path.getIngressTopics().isEmpty()
              ? path.getIngressTopic()
              : String.join(", ", path.getIngressTopics());
      System.out.printf("  Path: %s -> %s%n", ingressSummary, path.getEgressTopic());
      System.out.printf("    RA tree: %s%n", path.getAlgebraExpression());
      System.out.printf(
          "    flags: aggregated=%s joined=%s filtered=%s schemaChanged=%s%n",
          path.isAggregated(),
          path.isJoined(),
          path.isFiltered(),
          path.isSchemaChanged());
      System.out.printf("    input fields:            %s%n", path.getInputFields());
      System.out.printf("    output fields:           %s%n", path.getOutputFields());
      System.out.printf("    dropped fields:          %s%n", path.getDroppedFields());
      System.out.printf("    dropped sensitive:       %s%n", path.getDroppedSensitiveFields());
      System.out.printf(
          "    field sanitization ratio: %.2f (sensitive %.2f)%n",
          path.getFieldSanitizationRatio(),
          path.getSensitiveFieldSanitizationRatio());
      if (path.getSteps() != null && !path.getSteps().isEmpty()) {
        System.out.println("    steps:");
        for (final AppProcessingPolicy.RelationalAlgebraStep step : path.getSteps()) {
          System.out.printf(
              "      %s %s  in=%s out=%s%n",
              step.getAlgebraSymbol(),
              step.getDescription(),
              step.getInputFields(),
              step.getOutputFields());
        }
      }
    }
  }

  private static String shorten(final String value, final int max) {
    if (value == null) {
      return "";
    }
    if (value.length() <= max) {
      return value;
    }
    return value.substring(0, max - 3) + "...";
  }
}
