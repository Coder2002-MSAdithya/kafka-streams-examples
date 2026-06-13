package io.confluent.examples.streams.microservices;

import io.confluent.examples.streams.avro.microservices.Payment;
import io.confluent.examples.streams.avro.microservices.Product;
import io.confluent.examples.streams.microservices.domain.ProductCatalog;
import io.confluent.examples.streams.microservices.domain.Schemas;
import io.confluent.examples.streams.microservices.domain.beans.CreateOrderRequestBean;
import io.confluent.examples.streams.microservices.domain.beans.OrderBean;
import io.confluent.examples.streams.microservices.util.Paths;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.*;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

public class PostOrdersAndPayments {

    private static GenericType<OrderBean> newBean() {
        return new GenericType<OrderBean>() {
        };
    }

    private static KafkaProducer<String, Payment> buildPaymentProducer(final String bootstrapServers,
                                                                       final Properties defaultConfig) {
        final SpecificAvroSerializer<Payment> paymentSerializer = new SpecificAvroSerializer<>();

        paymentSerializer.configure(
                Schemas.buildSchemaRegistryConfigMap(defaultConfig),
                false);

        final Properties producerConfig = new Properties();
        producerConfig.putAll(defaultConfig);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 1);
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "payment-generator");

        return new KafkaProducer<>(producerConfig, new StringSerializer(), paymentSerializer);
    }
    private static void sendPayment(final String id,
                                    final Payment payment,
                                    final KafkaProducer<String, Payment> paymentProducer) {
        final ProducerRecord<String, Payment> record = new ProducerRecord<>("payments", id, payment);
        paymentProducer.send(record);
    }


    public static void main(final String[] args) throws Exception {

        final int NUM_CUSTOMERS = 6;
        final List<Product> productTypeList = new ArrayList<>(ProductCatalog.unitPrices().keySet());
        final Random randomGenerator = new Random();

        final Options opts = new Options();
        opts.addOption(Option.builder("b")
                    .longOpt("bootstrap-servers").hasArg().desc("Kafka cluster bootstrap server string").build())
                .addOption(Option.builder("s")
                    .longOpt("schema-registry").hasArg().desc("Schema Registry URL").build())
                .addOption(Option.builder("o")
                    .longOpt("order-service-url").hasArg().desc("Order Service URL").build())
                .addOption(Option.builder("c")
                    .longOpt("config-file").hasArg().desc("Java properties file with configurations for Kafka Clients").build())
                .addOption(Option.builder("h")
                    .longOpt("help").hasArg(false).desc("Show usage information").build());

        final CommandLine cl = new DefaultParser().parse(opts, args);
        if (cl.hasOption("h")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Post Orders and Payments", opts);
            return;
        }

        final String bootstrapServers = cl.getOptionValue("bootstrap-servers", DEFAULT_BOOTSTRAP_SERVERS);
        final String orderServiceUrl = cl.getOptionValue("order-service-url", "http://localhost:5432");
        final Properties defaultConfig = Optional.ofNullable(cl.getOptionValue("config-file", (String) null))
                .map(path -> {
                    try {
                        return buildPropertiesFromConfigFile(path);
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .orElse(new Properties());

        final String schemaRegistryUrl = cl.getOptionValue("schema-registry", DEFAULT_SCHEMA_REGISTRY_URL);
        defaultConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        Schemas.configureSerdes(defaultConfig);

        OrderBean returnedOrder;
        final Paths path = new Paths(orderServiceUrl);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(JacksonFeature.class);
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, 60000)
                    .property(ClientProperties.READ_TIMEOUT, 60000);
        final Client client = ClientBuilder.newClient(clientConfig);

        final KafkaProducer<String, Payment> paymentProducer = buildPaymentProducer(bootstrapServers, defaultConfig);

        int orderCount = 0;
        while (!Thread.currentThread().isInterrupted()) {

            final int randomCustomerId = randomGenerator.nextInt(NUM_CUSTOMERS);
            final Product randomProduct = productTypeList.get(randomGenerator.nextInt(productTypeList.size()));

            final CreateOrderRequestBean inputOrder = new CreateOrderRequestBean(
                    randomCustomerId,
                    randomProduct,
                    1);

            // POST order to OrdersService
            System.out.printf("Posting order to: %s   .... ", path.urlPost());
            try {
                final Response response = client.target(path.urlPost())
                        .request(APPLICATION_JSON_TYPE)
                        .post(Entity.json(inputOrder));
                System.out.printf("Response: %s %n", response.getStatus());

                final String orderId = orderIdFromLocation(response);
                if (orderId == null) {
                    System.err.println("POST succeeded but Location header missing order id");
                    response.close();
                    continue;
                }

                // GET the bean back explicitly
                System.out.printf("Getting order from: %s   .... ", path.urlGet(orderId));
                returnedOrder = client.target(path.urlGet(orderId))
                        .queryParam("timeout", Duration.ofMinutes(1).toMillis() / 2)
                        .request(APPLICATION_JSON_TYPE)
                        .get(newBean());
                response.close();
                if (returnedOrder.getCustomerId() != randomCustomerId
                    || returnedOrder.getProduct() != randomProduct
                    || returnedOrder.getQuantity() != 1) {
                    System.out.printf("Posted order does not match returned order: %s%n", returnedOrder);
                } else {
                    System.out.printf("Posted order %s equals returned order: %s%n", orderId, returnedOrder);
                }

                // Send payment
                final Payment payment = new Payment("Payment:1234", orderId, "CZK", 1000.00d);
                sendPayment(payment.getId(), payment, paymentProducer);
                orderCount++;
            } catch (final Exception ex) {
                System.err.printf("Error communicating with Orders Service, retrying shortly. %s", ex.getMessage());
            }

            Thread.sleep(5000L);
        }

        paymentProducer.flush();
        paymentProducer.close();
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

}
