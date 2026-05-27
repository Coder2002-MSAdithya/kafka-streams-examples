package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.Paths;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

/**
 * Posts a single order to OrdersService REST API ({@code POST /v1/orders}),
 * using the same JAX-RS client flow as {@link PostOrdersAndPayments}.
 */
public class PostOrderInteractive {

  private static GenericType<OrderBean> orderBeanType() {
    return new GenericType<OrderBean>() {
    };
  }

  public static void main(final String[] args) throws Exception {
    final Options opts = new Options();
    opts.addOption(Option.builder("o")
        .longOpt("order-service-url").hasArg()
        .desc("OrdersService base URL (default: http://localhost:5432)").build());
    opts.addOption(Option.builder("t")
        .longOpt("timeout-ms").hasArg()
        .desc("HTTP timeout for POST/GET in ms (default: 60000)").build());
    opts.addOption(Option.builder("g")
        .longOpt("get-after-post").hasArg(false)
        .desc("GET the order back after a successful POST").build());
    opts.addOption(Option.builder("h")
        .longOpt("help").hasArg(false).desc("Show usage").build());

    final CommandLine cl = new DefaultParser().parse(opts, args);
    if (cl.hasOption("h")) {
      new HelpFormatter().printHelp("PostOrderInteractive", opts);
      return;
    }

    final String orderServiceUrl = cl.getOptionValue("order-service-url", "http://localhost:5432");
    final long timeoutMs = Long.parseLong(cl.getOptionValue("timeout-ms", "60000"));
    final boolean getAfterPost = cl.hasOption("get-after-post");

    final BufferedReader in = new BufferedReader(
        new InputStreamReader(System.in, StandardCharsets.UTF_8));

    System.out.println("Submit an order to OrdersService at " + orderServiceUrl);
    System.out.println("Products: " + enumNames(Product.values()));
    System.out.println("States:   " + enumNames(OrderState.values()) + " (new orders usually CREATED)");
    System.out.println();

    final String id = prompt(in, "Order id", "1");
    final long customerId = Long.parseLong(prompt(in, "Customer id", "0"));
    final OrderState state = OrderState.valueOf(
        prompt(in, "State", OrderState.CREATED.name()).toUpperCase());
    final Product product = Product.valueOf(
        prompt(in, "Product", Product.JUMPERS.name()).toUpperCase());
    final int quantity = Integer.parseInt(prompt(in, "Quantity", "1"));
    final double price = Double.parseDouble(prompt(in, "Price", "99.99"));

    final OrderBean order = new OrderBean(id, customerId, state, product, quantity, price);
    final Paths paths = new Paths(orderServiceUrl);

    final ClientConfig clientConfig = new ClientConfig();
    clientConfig.register(JacksonFeature.class);
    clientConfig.property(ClientProperties.CONNECT_TIMEOUT, timeoutMs)
        .property(ClientProperties.READ_TIMEOUT, timeoutMs);

    final Client client = ClientBuilder.newClient(clientConfig);
    try {
      final String postUrl = paths.urlPost();
      System.out.printf("%nPosting to %s ...%n", postUrl);
      final Response response = client.target(postUrl)
          .queryParam("timeout", timeoutMs)
          .request(APPLICATION_JSON_TYPE)
          .post(Entity.json(order));

      final int status = response.getStatus();
      System.out.printf("POST response: %d %s%n", status, response.getStatusInfo().getReasonPhrase());
      if (response.getLocation() != null) {
        System.out.println("Location: " + response.getLocation());
      }
      final String body = response.hasEntity() ? response.readEntity(String.class) : "";
      if (!body.isEmpty()) {
        System.out.println(body);
      }
      response.close();

      if (status < 200 || status >= 300) {
        System.exit(1);
      }

      if (getAfterPost) {
        final String getUrl = paths.urlGet(id);
        System.out.printf("%nGET %s (timeout=%d ms) ...%n", getUrl, timeoutMs);
        final OrderBean returned = client.target(getUrl)
            .queryParam("timeout", timeoutMs)
            .request(APPLICATION_JSON_TYPE)
            .get(orderBeanType());
        System.out.println("Returned order: " + returned);
        if (!order.equals(returned)) {
          System.out.println("Warning: returned order does not match what was posted.");
        }
      }
    } finally {
      client.close();
    }
  }

  private static String enumNames(final Enum<?>[] values) {
    return Arrays.stream(values).map(Enum::name).collect(Collectors.joining(", "));
  }

  private static String prompt(final BufferedReader in, final String label, final String defaultValue)
      throws IOException {
    System.out.printf("%s [%s]: ", label, defaultValue);
    final String line = in.readLine();
    if (line == null || line.trim().isEmpty()) {
      return defaultValue;
    }
    return line.trim();
  }
}
