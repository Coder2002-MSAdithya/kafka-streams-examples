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

    /**
     * Returns a human-readable English explanation for why {@code requesterPrincipal} is not
     * on the allow-list for {@code capability} on {@code tagName}.  Used by the grantor to
     * emit a {@code [DIFC][GRANT-DECISION]} log line that can be included in reports verbatim.
     */
    public static String denyExplanation(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        if (capability == Capability.CAN_REMOVE) {
            if (TAG_ORDER.equals(tagName)) {
                if (PRINCIPAL_EMAIL.equals(requesterPrincipal)) {
                    return PRINCIPAL_EMAIL + " reads tagged order records solely to dispatch"
                            + " e-mail notifications and never calls declassifyTags on its"
                            + " produce path. Granting CAN_REMOVE would allow the email service"
                            + " to strip the 'order' tag from records without having sanitized"
                            + " the sensitive order fields (items, orderPrice), violating the"
                            + " declassification invariant. The DifcGrantPolicy allow-list"
                            + " therefore excludes email-svc from the CAN_REMOVE grantee set"
                            + " for tag 'order'.";
                }
                return requesterPrincipal + " is not in the CAN_REMOVE allow-list for tag"
                        + " 'order'. Only fraud-svc, inventory-svc, order-details-svc, and"
                        + " validations-agg-svc are authorised to produce with declassifyTags"
                        + " on the order tag.";
            }
            if (TAG_FRAUD.equals(tagName) || TAG_INV_VALID.equals(tagName)
                    || TAG_ORDER_VALID.equals(tagName)) {
                return requesterPrincipal + " is not in the CAN_REMOVE allow-list for tag '"
                        + tagName + "'. Only validations-agg-svc is authorised to aggregate"
                        + " and declassify validation tags.";
            }
            return requesterPrincipal + " is not authorised to hold CAN_REMOVE on tag '"
                    + tagName + "'.";
        }
        // CAN_ADD
        if (TAG_ORDER.equals(tagName)) {
            return requesterPrincipal + " is not in the CAN_ADD allow-list for tag 'order'."
                    + " Only the validator services and the aggregator are permitted to fetch"
                    + " tagged order records.";
        }
        return requesterPrincipal + " is not in the CAN_ADD allow-list for tag '" + tagName + "'.";
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
