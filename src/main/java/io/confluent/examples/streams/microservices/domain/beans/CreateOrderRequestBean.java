package io.confluent.examples.streams.microservices.domain.beans;

import io.confluent.examples.streams.avro.microservices.Product;

/**
 * REST request body for {@code POST /v1/orders}. Order id and price are assigned by OrdersService.
 */
public class CreateOrderRequestBean {

  private long customerId;
  private Product product;
  private int quantity;

  public CreateOrderRequestBean() {
  }

  public CreateOrderRequestBean(final long customerId, final Product product, final int quantity) {
    this.customerId = customerId;
    this.product = product;
    this.quantity = quantity;
  }

  public long getCustomerId() {
    return customerId;
  }

  public void setCustomerId(final long customerId) {
    this.customerId = customerId;
  }

  public Product getProduct() {
    return product;
  }

  public void setProduct(final Product product) {
    this.product = product;
  }

  public int getQuantity() {
    return quantity;
  }

  public void setQuantity(final int quantity) {
    this.quantity = quantity;
  }

  @Override
  public String toString() {
    return "CreateOrderRequestBean{"
        + "customerId=" + customerId
        + ", product=" + product
        + ", quantity=" + quantity
        + '}';
  }
}
