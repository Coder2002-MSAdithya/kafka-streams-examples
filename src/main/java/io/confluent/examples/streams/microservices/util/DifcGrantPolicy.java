package io.confluent.examples.streams.microservices.util;

import org.apache.kafka.clients.Capability;

/**
 * Allow-list for tag owners approving {@code GRANT_CAP} requests via {@code ADD_CLIENT_PRIVS}.
 * Requester identity is the authenticated SCRAM principal recorded by the broker on {@code GRANT_CAP}.
 *
 * <p>{@code CAN_ADD} — read tagged records ({@code addTag} on label).
 * {@code CAN_REMOVE} — {@code declassifyTags} on produce (validators strip {@code order};
 * aggregator strips {@code fraud}, {@code inv-valid}, {@code order-valid}, and {@code order}).
 */
public final class DifcGrantPolicy {

    public static final String PRINCIPAL_ORDERS = "orders-svc";
    public static final String PRINCIPAL_FRAUD = "fraud-svc";
    public static final String PRINCIPAL_INVENTORY = "inventory-svc";
    public static final String PRINCIPAL_ORDER_DETAILS = "order-details-svc";
    public static final String PRINCIPAL_EMAIL = "email-svc";
    public static final String PRINCIPAL_VALIDATIONS_AGG = "validations-agg-svc";

    public static final String TAG_ORDER = "order";
    public static final String TAG_FRAUD = "fraud";
    public static final String TAG_INV_VALID = "inv-valid";
    public static final String TAG_ORDER_VALID = "order-valid";

    private DifcGrantPolicy() {
    }

    public static boolean isAllowedGrant(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        if (tagName == null || requesterPrincipal == null || capability == null) {
            return false;
        }
        return switch (capability) {
            case CAN_ADD -> isAllowedAddGrant(tagName, requesterPrincipal);
            case CAN_REMOVE -> isAllowedRemoveGrant(tagName, requesterPrincipal);
        };
    }

    /**
     * Principal-only allow-list check (used before attested processing-policy verification for
     * {@code CAN_REMOVE}).
     */
    public static boolean isPrincipalAllowedGrant(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        return isAllowedGrant(tagName, requesterPrincipal, capability);
    }

    private static boolean isAllowedAddGrant(final String tagName, final String requesterPrincipal) {
        return switch (tagName) {
            case TAG_ORDER -> isOrderTagReader(requesterPrincipal);
            case TAG_FRAUD, TAG_INV_VALID, TAG_ORDER_VALID ->
                    PRINCIPAL_VALIDATIONS_AGG.equals(requesterPrincipal);
            default -> false;
        };
    }

    private static boolean isAllowedRemoveGrant(final String tagName, final String requesterPrincipal) {
        return switch (tagName) {
            case TAG_ORDER -> isOrderTagDeclassifier(requesterPrincipal);
            case TAG_FRAUD, TAG_INV_VALID, TAG_ORDER_VALID ->
                    PRINCIPAL_VALIDATIONS_AGG.equals(requesterPrincipal);
            default -> false;
        };
    }

    /** Principals that consume tagged {@code orders} (including join-only services). */
    private static boolean isOrderTagReader(final String requesterPrincipal) {
        return isOrderTagDeclassifier(requesterPrincipal)
                || PRINCIPAL_EMAIL.equals(requesterPrincipal);
    }

    /**
     * Principals that publish with {@code declassifyTags(order)} — validators and aggregator only.
     * {@link io.confluent.examples.streams.microservices.EmailService} reads {@code order} but does not declassify.
     */
    private static boolean isOrderTagDeclassifier(final String requesterPrincipal) {
        return PRINCIPAL_FRAUD.equals(requesterPrincipal)
                || PRINCIPAL_INVENTORY.equals(requesterPrincipal)
                || PRINCIPAL_ORDER_DETAILS.equals(requesterPrincipal)
                || PRINCIPAL_VALIDATIONS_AGG.equals(requesterPrincipal);
    }
}
