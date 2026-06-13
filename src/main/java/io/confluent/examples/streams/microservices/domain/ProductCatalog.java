package io.confluent.examples.streams.microservices.domain;

import io.confluent.examples.streams.avro.microservices.Product;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Server-side catalog of orderable products and their fixed unit prices (USD).
 */
public final class ProductCatalog {

  private static final Map<Product, Double> UNIT_PRICES;

  static {
    final Map<Product, Double> prices = new LinkedHashMap<>();
    prices.put(Product.JUMPERS, 75.00d);
    prices.put(Product.UNDERPANTS, 5.00d);
    prices.put(Product.STOCKINGS, 12.50d);
    prices.put(Product.SOCKS, 8.00d);
    prices.put(Product.SCARVES, 24.99d);
    UNIT_PRICES = Collections.unmodifiableMap(prices);
  }

  private ProductCatalog() {
  }

  public static Map<Product, Double> unitPrices() {
    return UNIT_PRICES;
  }

  public static double unitPrice(final Product product) {
    final Double price = UNIT_PRICES.get(product);
    if (price == null) {
      throw new IllegalArgumentException("Unknown product: " + product);
    }
    return price;
  }
}
