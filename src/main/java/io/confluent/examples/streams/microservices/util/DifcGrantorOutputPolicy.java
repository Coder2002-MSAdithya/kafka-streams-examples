package io.confluent.examples.streams.microservices.util;

import java.util.Collections;
import java.util.Set;

/**
 * Topics where a tag owner attaches a DIFC label tag when writing (including non-Streams producers).
 */
public final class DifcGrantorOutputPolicy {

    private DifcGrantorOutputPolicy() {
    }

    public static Set<String> outputTopicsFor(final String ownerPrincipal, final String tagName) {
        if (DifcGrantPolicy.PRINCIPAL_ORDERS.equals(ownerPrincipal)
                && DifcGrantPolicy.TAG_ORDER.equals(tagName)) {
            return Set.of("orders");
        }
        if (DifcGrantPolicy.PRINCIPAL_FRAUD.equals(ownerPrincipal)
                && DifcGrantPolicy.TAG_FRAUD.equals(tagName)) {
            return Set.of("order-validations");
        }
        if (DifcGrantPolicy.PRINCIPAL_INVENTORY.equals(ownerPrincipal)
                && DifcGrantPolicy.TAG_INV_VALID.equals(tagName)) {
            return Set.of("order-validations");
        }
        if (DifcGrantPolicy.PRINCIPAL_ORDER_DETAILS.equals(ownerPrincipal)
                && DifcGrantPolicy.TAG_ORDER_VALID.equals(tagName)) {
            return Set.of("order-validations");
        }
        return Collections.emptySet();
    }
}
