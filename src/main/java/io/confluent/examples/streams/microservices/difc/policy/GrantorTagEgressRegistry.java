package io.confluent.examples.streams.microservices.difc.policy;

import io.confluent.examples.streams.microservices.util.DifcGrantPolicy;

import java.util.Collections;
import java.util.Set;

/**
 * Topics on which each grantor principal attaches a given owned tag to produced records.
 */
public final class GrantorTagEgressRegistry {

    private GrantorTagEgressRegistry() {
    }

    public static Set<String> egressTopicsForTag(final String grantorPrincipal, final String ownedTag) {
        if (grantorPrincipal == null || ownedTag == null) {
            return Set.of();
        }
        if (DifcGrantPolicy.PRINCIPAL_ORDERS.equals(grantorPrincipal)
                && DifcGrantPolicy.TAG_ORDER.equals(ownedTag)) {
            return Set.of("orders");
        }
        if (DifcGrantPolicy.PRINCIPAL_FRAUD.equals(grantorPrincipal)
                && DifcGrantPolicy.TAG_FRAUD.equals(ownedTag)) {
            return Set.of("order-validations");
        }
        if (DifcGrantPolicy.PRINCIPAL_INVENTORY.equals(grantorPrincipal)
                && DifcGrantPolicy.TAG_INV_VALID.equals(ownedTag)) {
            return Set.of("order-validations");
        }
        if (DifcGrantPolicy.PRINCIPAL_ORDER_DETAILS.equals(grantorPrincipal)
                && DifcGrantPolicy.TAG_ORDER_VALID.equals(ownedTag)) {
            return Set.of("order-validations");
        }
        return Collections.emptySet();
    }
}
