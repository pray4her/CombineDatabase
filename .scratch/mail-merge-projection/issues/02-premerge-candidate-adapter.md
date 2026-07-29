# 02 — Pre-merge candidate evidence adapter

**What to build:** From the pre-merge, split-and-deduplicated paper-side AuthorContact store, assemble enriched candidates (contact + paper evidences including title/venue/year, author_order, corresponding-author signal, citations when available) so they can be passed into the MailMergeProjection seam. Operators/engineers can obtain seam-ready candidates without hand-joining fields; cleaning `records` remain unpolluted by marketing columns.

**Blocked by:** 01 — Core seam: MailSendGate + EmailAnchorSelection + MailMergeProjection

**Status:** ready-for-agent

- [x] Adapter loads AuthorContact rows from the pre-merge deduplicated paper database
- [x] Each candidate includes resolvable EmailAnchorPublication evidence needed by the seam (at least title when present in source/silver linkage; author_order and corresponding-author flag when derivable)
- [x] Evidence assembly may use source-row linkage and/or silver occurrence/claim/publication fields; ingest dedupe rules are not reused as EmailAnchorSelection
- [x] Contacts whose title cannot be resolved are still passable into the seam (which will gate them out for missing anchor_title)
- [x] Adapter output shape matches what ticket 01’s seam expects; no Person/match-stage dependency
- [x] Light smoke or fixture check that a small sample DB/source set yields non-empty candidate structures suitable for the seam

