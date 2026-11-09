---
title: inventory_example generated dependency lineage
description: Explicit dependency edges; not service-execution evidence
ms.topic: reference
---

## Declared edges

* `inventory.bronze.products` -> `inventory.silver.products` (`succeeded`)
* `inventory.bronze.movements` -> `inventory.silver.movements` (`succeeded`)
* `inventory.silver.products` -> `inventory.silver.movements` (`succeeded`)
* `inventory.silver.products` -> `gold.dim_product` (`source_committed`)
* `inventory.silver.movements` -> `gold.fact_inventory_movement` (`source_committed`)
* `gold.dim_product` -> `gold.fact_inventory_movement` (`succeeded`)
* `gold.fact_inventory_movement` -> `gold.dim_date` (`succeeded`)
* `gold.fact_inventory_movement` -> `gold.dim_fiscal_period` (`succeeded`)
* `gold.fact_inventory_movement` -> `gold.mlv_inventory_movement_monthly` (`succeeded`)
