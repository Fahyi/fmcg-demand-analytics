# Data Dictionary

## Raw Dataset

| Column | Type | Description |
|---|---|---|
| `date` | Date | Sales date |
| `sku` | String | Stock keeping unit |
| `brand` | String | Product brand |
| `segment` | String | Product segment |
| `category` | String | FMCG product category |
| `channel` | String | Sales channel |
| `region` | String | Sales region |
| `pack_type` | String | Package type |
| `price_unit` | Numeric | Unit price |
| `promotion_flag` | Integer | 1 = promoted, 0 = not promoted |
| `delivery_days` | Integer | Delivery lead time in days |
| `stock_available` | Integer | Available stock quantity |
| `delivered_qty` | Integer | Delivered quantity |
| `units_sold` | Integer | Units sold |

## Derived Fields

| Field | Formula | Description |
|---|---|---|
| `revenue` | `price_unit * units_sold` | Sales revenue |
| `avg_selling_price` | `revenue / units_sold` | Average realized selling price |
| `sell_through_rate` | `units_sold / stock_available` | Sales relative to available stock |
| `delivery_gap` | `delivered_qty - units_sold` | Difference between delivered and sold quantity |
| `data_quality_issue_flag` | quality flags | Identifies negative or invalid values |

## Data Quality Treatment

The raw dataset contains some negative values in operational quantity fields. The pipeline preserves original columns and creates cleaned versions:

- negative `units_sold` → 0 in cleaned metric
- negative `stock_available` → 0 in cleaned metric
- negative `delivered_qty` → 0 in cleaned metric

Quality flags are also created so the issue remains auditable.
