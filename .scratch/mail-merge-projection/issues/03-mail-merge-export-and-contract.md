# 03 — MailMerge export + template column contract

**What to build:** End-to-end operator path: run adapter + seam and write a separate MailMergeProjection CSV/XLSX suitable for mail-merge tools, plus a short column-placement contract (salutation / paper hook / relevance bridge / fixed CTA / audit-only fields). Cleaning database is not written back.

**Blocked by:** 02 — Pre-merge candidate evidence adapter

**Status:** ready-for-agent

- [x] Command or export entry produces a MailMergeProjection file (CSV and/or XLSX) outside the cleaning store
- [x] File columns match the template + audit contract from the spec / ticket 01
- [x] Gated-out contacts are omitted from the main export (optional reject summary allowed, not required)
- [x] Documentation states: salutation ← recipient_name; hook ← anchor_title + anchor_source_title + anchor_year; bridge ← research_area_primary (optional institution); CTA ← fixed business copy; audit fields not for prose
- [x] Domain terms AuthorContact, EmailAnchorPublication, EmailAnchorSelection, MailMergeProjection, MailSendGate are used in the operator-facing notes
- [x] Demoable path: point at a sample pre-merge DB (with resolvable evidence) → open the export and see personalized columns populated

