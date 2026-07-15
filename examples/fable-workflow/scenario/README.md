# usage-report

Rolls up API calls per day for the billing page.

## Rules

- Days are **UTC days**. Billing buckets by UTC and is the system of record.
- Timestamps arrive ISO-8601 with an explicit UTC offset.
