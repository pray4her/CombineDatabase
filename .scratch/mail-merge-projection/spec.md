# MailMergeProjection for AuthorContact outreach

Status: ready-for-agent

## Problem Statement

Outreach emails today only personalize with a name variable, while the send list comes from the pre-merge, split-and-deduplicated paper-side database (AuthorContact records), not the merged Person/expert database. That paper-side data already contains (or can resolve) institution, research areas, and a concrete Publication that motivated the contact — but there is no audited, template-ready projection that selects one EmailAnchorPublication per recipient, applies send gates, and exposes stable merge fields. Without this, personalization either stays too thin or risks polluting the cleaning database and citing the wrong paper.

## Solution

Add a MailMergeProjection pipeline step/view that, for each eligible AuthorContact, selects exactly one EmailAnchorPublication via EmailAnchorSelection, applies MailSendGate filters, and emits a flat, template-ready row set separate from the cleaning `records` store. Email copy uses recipient_name in the salutation; anchor_title + anchor_source_title + anchor_year as the opening hook; research_area_primary (optional institution) as the relevance bridge; and fixed business CTA copy for the body. Gate/ranking fields remain on the projection for filtering and audit, not in marketing prose.

## User Stories

1. As an outreach operator, I want a MailMergeProjection export, so that I can fill an email template without hand-joining paper fields.
2. As an outreach operator, I want each row to represent one AuthorContact, so that I do not confuse pre-merge contacts with merged Person experts.
3. As an outreach operator, I want exactly one EmailAnchorPublication per recipient, so that the opening hook cites a specific paper instead of a vague multi-paper summary.
4. As an outreach operator, I want EmailAnchorSelection to prefer corresponding authors, so that the cited paper is the one most tied to the email binding.
5. As an outreach operator, I want EmailAnchorSelection to prefer smaller author_order when corresponding-author status ties, so that earlier-listed authors are favored.
6. As an outreach operator, I want newer publication_year to break remaining ties, so that outreach cites recent work when possible.
7. As an outreach operator, I want higher citation counts to break remaining ties when year ties, so that the more visible paper is preferred when available.
8. As an outreach operator, I want higher name–email similarity to be the final tie-break, so that weakly matched alternatives lose to stronger bindings.
9. As an outreach operator, I want EmailAnchorSelection to be independent of ingest dedupe survival rules, so that “who may be emailed” and “which paper to cite” stay separate decisions.
10. As an outreach operator, I want MailSendGate to require recipient_name and email, so that incomplete contacts never enter the merge file.
11. As an outreach operator, I want MailSendGate to require anchor_title, so that every merge row can support a paper hook.
12. As an outreach operator, I want contacts with failed email_validity to be excluded, so that known-bad addresses are not mailed.
13. As an outreach operator, I want contacts with unknown or passed email_validity to remain eligible, so that unvalidated-but-plausible addresses are not over-filtered.
14. As an outreach operator, I want a configurable minimum similarity (default 0.6), so that weak name–email bindings can be dropped without code changes.
15. As an outreach operator, I want recipient_name to prefer full_name over short_name, so that salutations use the most complete available name.
16. As an outreach operator, I want anchor_source_title and anchor_year on each row, so that the hook can name venue and timing.
17. As an outreach operator, I want research_area_primary derived from research_areas (with wos_categories fallback), so that the relevance bridge uses one domain term, not a keyword dump.
18. As an outreach operator, I want institution (preferring institution_norm) and country on the projection, so that templates can optionally bridge via affiliation/geography.
19. As an outreach operator, I want similarity, email_validity, is_corresponding_author, author_order, and times_cited retained as non-prose columns, so that I can audit and re-filter without burying them in email copy.
20. As an outreach operator, I want ethnic_chinese and QS/Fortune ranks excluded from default template variables, so that sensitive or gimmicky signals are not mailed by default.
21. As an outreach operator, I want the projection stored/exported separately from the cleaning database, so that marketing fields do not pollute AuthorContact source of truth.
22. As an outreach operator, I want CSV/XLSX output suitable for mail-merge tools, so that existing sending workflows keep working.
23. As an outreach operator, I want deterministic selection given the same candidate set and config, so that reruns are auditable and comparable.
24. As an outreach operator, I want rows that fail the gate to be omitable (and optionally reportable as rejects), so that I can see why volume dropped.
25. As a data engineer, I want adapters to assemble candidate evidence (author_order, corresponding flag, title, venue, year, citations) from the pre-merge paper DB and source/silver fields, so that the pure projection seam stays free of I/O details.
26. As a data engineer, I want missing optional bridge fields (institution/country) to still allow emission when gates pass, so that incomplete affiliation data does not block a valid paper hook.
27. As a data engineer, I want abstract text and long keyword lists excluded from default merge columns, so that templates stay short and compliant with outreach best practice.
28. As a template author, I want a documented column contract for salutation / hook / bridge / CTA / audit fields, so that copy placement stays consistent.
29. As a template author, I want the CTA body to remain business-fixed (not DB-driven), so that offer language can change without re-deriving paper fields.
30. As a reviewer, I want domain terms AuthorContact, EmailAnchorPublication, EmailAnchorSelection, MailMergeProjection, and MailSendGate used in the implementation docs, so that language stays aligned with CONTEXT.md.
31. As a future maintainer, I want Person-based outreach left out of this feature, so that merging experts is not mixed into the pre-merge mail path.
32. As a QA engineer, I want fixture-driven tests on ranking and gating only through the projection seam, so that behavior is locked without spinning up mail servers or OpenSearch.

## Implementation Decisions

- **Domain vocabulary**: Use CONTEXT.md terms — AuthorContact (recipient), EmailAnchorPublication (single cited Publication), EmailAnchorSelection (ranking rule A), MailMergeProjection (delivery view), MailSendGate (hard filters). Do not call the recipient Person/Expert in this feature.
- **Primary seam (only required test surface)**: `build_mail_merge_projection(candidates, *, similarity_threshold=0.6) -> projection_rows` (name may vary slightly; responsibility must not). Input is an in-memory structure: each AuthorContact with a list of candidate paper evidences already enriched. Output is MailMergeProjection rows. File I/O, GUI, SMTP, OpenSearch, and ingest dedupe are outside this seam.
- **EmailAnchorSelection dictionary order** (higher priority first):
  1. corresponding author (`is_corresponding_author` / reprint-equivalent true)
  2. smaller `author_order`
  3. newer publication year
  4. higher times cited when present
  5. higher name–email similarity
- **MailSendGate**:
  - require non-empty recipient_name and email
  - require non-empty anchor_title after selection
  - exclude email_validity values that mean failed validation; allow passed and unknown
  - require similarity >= configurable threshold (default 0.6)
- **Projection columns (template + audit)**:
  - Template: `recipient_name`, `email`, `anchor_title`, `anchor_source_title`, `anchor_year`, `research_area_primary`, `institution`, `country`
  - Audit/gate (not for marketing prose by default): `similarity`, `email_validity`, `is_corresponding_author`, `author_order`, `times_cited`
- **recipient_name**: `full_name` if present else `short_name`.
- **research_area_primary**: first usable token from `research_areas`, else fallback from `wos_categories`; single value only.
- **institution**: prefer `institution_norm` over raw `institution`.
- **Copy placement contract** (documentation for template authors, not runtime email send):
  - Salutation → `recipient_name`
  - Opening hook → `anchor_title` + `anchor_source_title` + `anchor_year`
  - Relevance bridge → `research_area_primary`, optional `institution`
  - Body CTA → fixed business copy
  - Do not place gate/audit fields in prose
- **Persistence**: Emit a separate export/view (CSV/XLSX or equivalent). Do not add marketing columns back onto the cleaning `records` table as the primary design.
- **Adapters (thin, untested or lightly smoke-tested)**: Load AuthorContact candidates from the pre-merge deduplicated paper DB; resolve Publication fields and author_order / corresponding flags from source row linkage and/or silver claim/occurrence entities when needed; call the seam; write the projection file.
- **Config**: similarity threshold must be injectable at the seam; default 0.6.
- **Determinism**: Same candidate set + threshold → same selected anchors and row set (stable sort with explicit final tie-break, e.g. original candidate id/order).
- **Rejects**: Rows failing the gate are omitted from the main projection; optional reject summary is allowed but not required for MVP.
- **No schema change to Person / match stage** for this feature.
- **No default inclusion** of `ethnic_chinese`, QS Top 200, or Fortune/World Top 500 fields in the merge template contract.

## Testing Decisions

- Good tests assert observable outputs of `build_mail_merge_projection` only: which rows emit, which EmailAnchorPublication was chosen, and which column values appear — not private helpers, SQL, or file formats.
- Test the projection module/seam; do not require OpenSearch, DashScope, SMTP, or GUI.
- Prior art: existing `unittest` pure-function suites under `tests/` (e.g. identity classification and NL aggregate tests).
- Minimum cases:
  - Gate drops missing name/email/title, failed validity, and below-threshold similarity
  - Unknown validity still eligible when other gates pass
  - Selection prefers corresponding author over non-corresponding despite older year
  - Among non-corresponding (or equal corresponding), smaller author_order wins
  - Year, citations, then similarity apply in order when earlier keys tie
  - recipient_name falls back from full_name to short_name
  - research_area_primary falls back from research_areas to wos_categories
  - institution prefers institution_norm
  - Audit columns present; ethnic_chinese / ranking enrichment absent from default row contract
  - Deterministic tie-break when all ranking keys equal

## Out of Scope

- Sending email, SMTP validation runs, bounce handling, or ESP integration
- Merged Person / scholars-expert outreach paths
- Multi-paper hooks or research-summary style aggregation in one email
- Writing marketing fields back into the cleaning `records` table
- Co-author graph, NL expert search, OpenSearch indexing, or vector pipeline changes
- Default use of ethnic_chinese, QS, or Fortune signals in template copy
- Manual UI for picking an alternate EmailAnchorPublication (selection rule C from grilling)
- Changing ingest dedupe algorithms (`(short_name, email)` or per-email max similarity)

## Further Notes

- Shared understanding from grilling is recorded in root `CONTEXT.md` (AuthorContact, EmailAnchorPublication, EmailAnchorSelection, MailMergeProjection, MailSendGate).
- Pre-merge `records` often lack title/author_order; adapters must resolve EmailAnchorPublication evidence before the seam. If title cannot be resolved, MailSendGate excludes the contact.
- Ingest dedupe answers “which AuthorContact survives”; EmailAnchorSelection answers “which paper to cite” — do not substitute one for the other.
- Optional follow-up (not in this spec): ADR for EmailAnchorSelection vs dedupe-survivor anchoring; reject-report export; per-campaign threshold profiles.
