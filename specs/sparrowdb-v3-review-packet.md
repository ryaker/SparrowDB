# SparrowDB v3 Review Packet

**Version:** v3.0-draft  
**Date:** March 20, 2026  
**Read with:** `specs/sparrowdb-v3-implementation-spec.md` and `specs/review-pass-2-disposition.md`

## Review Goal

Pressure-test the v3 synthesis for:

- hidden correctness risk
- byte-layout mistakes
- replay and recovery ambiguity
- places where the baseline thesis is still vulnerable to accidental scope drift

## What Changed Since v2

- fresh-database bootstrap is now explicit
- catalog payload TLV format is now defined
- encryption AAD now binds file identity
- torn-page recovery policy is now explicit
- WAL encryption is now defined at record-payload level
- parser/binder syntax and milestone gates are sharper

## Questions Every Reviewer Should Answer

1. What are the top 5 remaining implementation risks?
2. Which byte layouts are still too loose to code safely?
3. Is the `last_applied_lsn` payload-prefix design sufficient for idempotent replay?
4. Is the encrypted-page contract sound enough for v1?
5. Is the distinct-`edge_id` choice for undirected syntax the right v1 simplification?
6. Which requirements are still too vague to test?
7. What would you cut only if schedule compression becomes unavoidable?
8. Does the spec still faithfully preserve the March 20 architecture baseline?

## Highest-Value Hotspots

1. Recovery correctness across partially applied multi-page WAL records
2. Encryption AAD and wrong-key behavior
3. Binary ASP-Join multiplicity semantics
4. `OPTIMIZE` rewrite semantics and sorted-flag guarantees
5. Catalog payload format stability and whether it is now sufficient to freeze

## Preferred Reviewer Output

1. Findings
2. Ambiguities
3. Concrete Edits
4. Scope Cuts Only If Necessary
5. Confidence
