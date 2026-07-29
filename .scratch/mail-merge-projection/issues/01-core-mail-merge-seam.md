# 01 — Core seam: MailSendGate + EmailAnchorSelection + MailMergeProjection

**What to build:** Given in-memory AuthorContact candidates (each with one or more paper evidences), produce MailMergeProjection rows: apply MailSendGate, pick exactly one EmailAnchorPublication via EmailAnchorSelection rule A, and emit the agreed template + audit columns. Behaviour is fully verifiable with unit tests—no real database, mail send, or OpenSearch.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [x] `build_mail_merge_projection` (or equivalent single seam) accepts candidates plus configurable `similarity_threshold` (default 0.6) and returns projection rows deterministically
- [x] MailSendGate drops rows missing recipient_name or email, missing anchor_title after selection, failed email_validity, or similarity below threshold; allows passed and unknown validity
- [x] EmailAnchorSelection uses dictionary order: corresponding author → smaller author_order → newer year → higher times_cited → higher similarity, with a stable final tie-break
- [x] Projection includes template columns (recipient_name, email, anchor_title, anchor_source_title, anchor_year, research_area_primary, institution, country) and audit columns (similarity, email_validity, is_corresponding_author, author_order, times_cited)
- [x] recipient_name prefers full_name over short_name; research_area_primary prefers research_areas then wos_categories; institution prefers institution_norm
- [x] Default row contract excludes ethnic_chinese and QS/Fortune fields
- [x] Unit tests cover gating, ranking, field fallbacks, and deterministic ties via the seam only (unittest, matching existing test style)

