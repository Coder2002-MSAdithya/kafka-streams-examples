# DIFC grant and data-flow status — kafka-streams-examples

Generated from workflow logs under `/tmp/kafka-streams-examples-difc-logs`.

## kafka-streams-examples
Logs: `/tmp/kafka-streams-examples-difc-logs` (present)

### Data flow
| Metric | Value |
|--------|-------|
| orders/users posted | 13 |
| validated events | 389 |
| failed events | 2 |
| aggregation decisions | 35 |

### Capability grants (grantor decision)
| Grantor | Requester | Tag | CAN_ADD | CAN_REMOVE | Notes |
|--------|-----------|-----|---------|------------|-------|
| FraudService | validations-agg-svc | fraud | GRANTED | GRANTED |  |
| InventoryService | validations-agg-svc | inv-valid | GRANTED | GRANTED |  |
| OrdersService | email-svc | order | GRANTED | - |  |
| OrdersService | fraud-svc | order | GRANTED | GRANTED |  |
| OrdersService | inventory-svc | order | GRANTED | GRANTED |  |
| OrdersService | order-details-svc | order | GRANTED | GRANTED |  |
| OrdersService | validations-agg-svc | order | GRANTED | DENIED | output relation orders → orders Sink(orders, ∪(π_k[π:_aggregate_value, |
| OrderDetailsService | validations-agg-svc | order-valid | GRANTED | GRANTED |  |

### External connections
| Principal | Target | Allowed | Expected? |
|-----------|--------|---------|-----------|
| email-svc | localhost:8081 | True | Review — verify against service code |
| fraud-svc | - | - | OK (broker-only) |
| inventory-svc | - | - | OK (broker-only) |
| order-details-svc | - | - | OK (broker-only) |
| orders-svc | - | - | OK (broker-only) |
| validations-agg-svc | - | - | OK (broker-only) |
