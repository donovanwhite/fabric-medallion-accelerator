---
title: Shared Gold engine
description: Reusable dimension, fact, release and aggregate mechanisms without customer business defaults
ms.date: 2026-11-09
ms.topic: reference
---

## Shared responsibilities

The Gold engine prepares committed/version-pinned inputs, builds declared
dimensions and atomic/position facts, resolves dependency versions, checks
quality/reconciliation and finalizes complete technical releases.
It preserves event correction, observed-time SCD2 and exact release
membership; it does not infer a business model from column names.

Gold inventories, mappings, source-specific enrichments and fiscal calendars
are selected case contracts/extensions. Domain models and implementation
references live in each case's specifications, not as architecture defaults.

## Packaging and lifecycle

Build the selected case's generic Gold parent/worker and MLV preview under
its `generated/notebooks` directory. They reuse these sources and package
the case's reviewed code separately, without active source/Gold arrays.
The neutral shared module names do not select a case.

MLV definition changes, refresh jobs and aggregate readiness use explicit
versioned lifecycle controls, real serving ownership and actual-mode
evidence. Preview is not authorization. Layout changes and business
publication remain separately gated.
