package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.ProductCatalog;
import io.confluent.examples.streams.microservices.domain.beans.CreateOrderRequestBean;
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
import java.util.Map;

import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

/**
 * Posts a single order to OrdersService REST API ({@code POST /v1/orders}).
 * Order id and unit price are assigned by the server from {@link ProductCatalog}.
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
    System.out.println("Order id and price are assigned by the server.");
    System.out.println("Products (fixed unit prices):");
    for (final Map.Entry<Product, Double> entry : ProductCatalog.unitPrices().entrySet()) {
      System.out.printf("  %-12s $%.2f%n", entry.getKey().name(), entry.getValue());
    }
    System.out.println();

    final long customerId = Long.parseLong(prompt(in, "Customer id", "0"));
    final Product product = Product.valueOf(
        prompt(in, "Product", Product.JUMPERS.name()).toUpperCase());
    final int quantity = Integer.parseInt(prompt(in, "Quantity", "1"));

    final CreateOrderRequestBean request = new CreateOrderRequestBean(customerId, product, quantity);
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
          .post(Entity.json(request));

      final int status = response.getStatus();
      System.out.printf("POST response: %d %s%n", status, response.getStatusInfo().getReasonPhrase());
      if (response.getLocation() != null) {
        System.out.println("Location: " + response.getLocation());
      }
      final String body = response.hasEntity() ? response.readEntity(String.class) : "";
      if (!body.isEmpty()) {
        System.out.println(body);
      }

      if (status < 200 || status >= 300) {
        response.close();
        System.exit(1);
      }

      final String orderId = orderIdFromLocation(response);
      response.close();

      if (orderId == null) {
        System.err.println("Warning: POST succeeded but Location header did not contain an order id.");
      } else {
        System.out.println("Assigned order id: " + orderId);
      }

      if (getAfterPost && orderId != null) {
        final String getUrl = paths.urlGet(orderId);
        System.out.printf("%nGET %s (timeout=%d ms) ...%n", getUrl, timeoutMs);
        final OrderBean returned = client.target(getUrl)
            .queryParam("timeout", timeoutMs)
            .request(APPLICATION_JSON_TYPE)
            .get(orderBeanType());
        System.out.println("Returned order: " + returned);
        if (!orderId.equals(returned.getId())) {
          System.out.println("Warning: returned order id does not match Location header.");
        }
        if (returned.getProduct() != product || returned.getCustomerId() != customerId
            || returned.getQuantity() != quantity) {
          System.out.println("Warning: returned order fields do not match the request.");
        }
        final double expectedPrice = ProductCatalog.unitPrice(product);
        if (Double.compare(returned.getPrice(), expectedPrice) != 0) {
          System.out.printf("Warning: expected server price %.2f but got %.2f%n",
              expectedPrice, returned.getPrice());
        }
      }
    } finally {
      client.close();
    }
  }

  private static String orderIdFromLocation(final Response response) {
    if (response.getLocation() == null) {
      return null;
    }
    final String path = response.getLocation().getPath();
    final int lastSlash = path.lastIndexOf('/');
    if (lastSlash < 0 || lastSlash == path.length() - 1) {
      return null;
    }
    return path.substring(lastSlash + 1);
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
