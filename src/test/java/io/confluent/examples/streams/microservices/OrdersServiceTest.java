package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.OrderState;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.ProductCatalog;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.Schemas.Topics;
import io.confluent.examples.streams.microservices.domain.beans.CreateOrderRequestBean;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.MicroserviceTestUtils;
import io.confluent.examples.streams.microservices.util.Paths;

import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;
import java.io.File;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static io.confluent.examples.streams.avro.microservices.Order.newBuilder;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.assertj.core.api.AssertionsForClassTypes.within;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.fail;

public class OrdersServiceTest extends MicroserviceTestUtils {

  private OrdersService rest;
  private OrdersService rest2;

  @BeforeClass
  public static void startKafkaCluster() {
    final Properties config = new Properties();
    config.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
    Schemas.configureSerdes(config);
  }

  @After
  public void shutdown() {
    if (rest != null) {
      rest.stop();
      rest.cleanLocalState();
    }
    if (rest2 != null) {
      rest2.stop();
      rest2.cleanLocalState();
    }
  }

  @Before
  public void prepareKafkaCluster() throws Exception {
    CLUSTER.deleteTopicsAndWait(60000, Topics.ORDERS.name(), "OrdersService-orders-store-changelog");
    CLUSTER.createTopic(Topics.ORDERS.name());

    final Properties config = new Properties();
    config.put(SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());

    Schemas.configureSerdes(config);
  }

  @Test
  public void shouldPostOrderAndGetItBack() {
    final CreateOrderRequestBean request = new CreateOrderRequestBean(2L, Product.JUMPERS, 10);
    final double expectedPrice = ProductCatalog.unitPrice(Product.JUMPERS);

    final Client client = ClientBuilder.newClient();

    rest = new OrdersService("localhost");
    rest.start(CLUSTER.bootstrapServers(), TestUtils.tempDirectory().getPath(), new Properties());
    final Paths paths = new Paths("localhost", rest.port());

    final Response response = postWithRetries(
      client.target(paths.urlPost()).request(APPLICATION_JSON_TYPE),
      Entity.json(request),
      5);

    assertThat(response.getStatus()).isEqualTo(HttpURLConnection.HTTP_CREATED);
    final String orderId = orderIdFromLocation(response);
    final java.net.URI location = response.getLocation();
    response.close();
    assertThat(orderId).isNotNull();

    Invocation.Builder builder = client
      .target(location)
      .queryParam("timeout", Duration.ofSeconds(30).toMillis())
      .request(APPLICATION_JSON_TYPE);

    OrderBean returnedBean = getWithRetries(builder, newBean(), 5);

    assertThat(returnedBean.getId()).isEqualTo(orderId);
    assertThat(returnedBean.getCustomerId()).isEqualTo(2L);
    assertThat(returnedBean.getProduct()).isEqualTo(Product.JUMPERS);
    assertThat(returnedBean.getQuantity()).isEqualTo(10);
    assertThat(returnedBean.getPrice()).isCloseTo(expectedPrice, within(0.001));
    assertThat(returnedBean.getState()).isEqualTo(OrderState.CREATED);

    builder = client
      .target(paths.urlGet(orderId))
      .queryParam("timeout", Duration.ofSeconds(30).toMillis())
      .request(APPLICATION_JSON_TYPE);

    returnedBean = getWithRetries(builder, newBean(), 5);
    assertThat(returnedBean.getId()).isEqualTo(orderId);
    assertThat(returnedBean.getPrice()).isCloseTo(expectedPrice, within(0.001));
  }


  @Test
  public void shouldGetValidatedOrderOnRequest() {
    final CreateOrderRequestBean request = new CreateOrderRequestBean(3L, Product.JUMPERS, 10);

    final Client client = ClientBuilder.newClient();

    rest = new OrdersService("localhost");
    rest.start(CLUSTER.bootstrapServers(), TestUtils.tempDirectory().getPath(), new Properties());
    final Paths paths = new Paths("localhost", rest.port());

    final Response postResponse = postWithRetries(
        client.target(paths.urlPost()).request(APPLICATION_JSON_TYPE), Entity.json(request), 5);
    final String orderId = orderIdFromLocation(postResponse);
    postResponse.close();

    final Order orderV1 = new Order(
        orderId,
        request.getCustomerId(),
        OrderState.CREATED,
        request.getProduct(),
        request.getQuantity(),
        ProductCatalog.unitPrice(request.getProduct()));

    MicroserviceTestUtils.sendOrders(Collections.singletonList(
      newBuilder(orderV1)
        .setState(OrderState.VALIDATED)
        .build()));

    final Invocation.Builder builder = client
      .target(paths.urlGetValidated(orderId))
      .queryParam("timeout", Duration.ofSeconds(30).toMillis())
      .request(APPLICATION_JSON_TYPE);

    final OrderBean returnedBean = getWithRetries(builder, newBean(), 5);

    assertThat(returnedBean.getState()).isEqualTo(OrderState.VALIDATED);
  }

  @Test
  public void shouldTimeoutGetIfNoResponseIsFound() {
    final Client client = ClientBuilder.newClient();

    rest = new OrdersService("localhost");
    rest.start(CLUSTER.bootstrapServers(), TestUtils.tempDirectory().getPath(), new Properties());
    final Paths paths = new Paths("localhost", rest.port());

    final Invocation.Builder builder = client
      .target(paths.urlGet("missing-order-id"))
      .queryParam("timeout", Duration.ofMillis(100).toMillis())
      .request(APPLICATION_JSON_TYPE);

    try {
      getWithRetries(builder, newBean(), 0);
      fail("Request should have failed as materialized view has not been updated");
    } catch (final ServerErrorException e) {
      assertThat(e.getMessage()).isEqualTo("HTTP 504 Gateway Timeout");
    }
  }

  @Test
  public void shouldGetOrderByIdWhenOnDifferentHost() {
    final CreateOrderRequestBean request = new CreateOrderRequestBean(4L, Product.JUMPERS, 10);
    final Client client = ClientBuilder.newClient();

    rest = new OrdersService("localhost");
    rest.start(
            CLUSTER.bootstrapServers(),
            TestUtils.tempDirectory().getPath() + File.separator + "instance-1",
            new Properties()
    );
    final Paths paths1 = new Paths("localhost", rest.port());
    rest2 = new OrdersService("localhost");
    rest2.start(
            CLUSTER.bootstrapServers(),
            TestUtils.tempDirectory().getPath() + File.separator + "instance-2",
            new Properties()
    );
    final Paths paths2 = new Paths("localhost", rest2.port());

    final Response postResponse = postWithRetries(
        client.target(paths1.urlPost()).request(APPLICATION_JSON_TYPE), Entity.json(request), 5);
    final String orderId = orderIdFromLocation(postResponse);
    postResponse.close();

    Invocation.Builder builder = client
      .target(paths1.urlGet(orderId))
      .queryParam("timeout", Duration.ofSeconds(30).toMillis())
      .request(APPLICATION_JSON_TYPE);
    OrderBean returnedOrder = getWithRetries(builder, newBean(), 5);

    assertThat(returnedOrder.getId()).isEqualTo(orderId);
    assertThat(returnedOrder.getCustomerId()).isEqualTo(4L);
    assertThat(returnedOrder.getProduct()).isEqualTo(Product.JUMPERS);

    builder = client
      .target(paths2.urlGet(orderId))
      .queryParam("timeout", Duration.ofSeconds(30).toMillis())
      .request(APPLICATION_JSON_TYPE);
    returnedOrder = getWithRetries(builder, newBean(), 5);

    assertThat(returnedOrder.getId()).isEqualTo(orderId);
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

  private GenericType<OrderBean> newBean() {
    return new GenericType<OrderBean>() {};
  }
}
