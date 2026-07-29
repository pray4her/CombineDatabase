# MailMergeProjection operator guide

`MailMergeProjection` is a separate export for paper-side `AuthorContact` outreach. It does not write marketing fields into the pre-merge `records` database and does not use merged Person/expert records.

## Export

```powershell
python .\export_mail_merge.py --db-path .\records.db --output .\mail_merge.csv --similarity-threshold 0.6
```

CSV is always supported. Use an `.xlsx` output name when `pandas` is installed to write an Excel workbook. `MailSendGate` omits contacts without a recipient name, email, or selected anchor title; known failed email validation; and contacts below the configured similarity threshold.

## Template column placement contract

| Placement | MailMergeProjection columns | Rule |
| --- | --- | --- |
| Salutation | `recipient_name` | Address the AuthorContact by its full name when available. |
| Opening hook | `anchor_title`, `anchor_source_title`, `anchor_year` | Cite the single `EmailAnchorPublication` selected by `EmailAnchorSelection`. |
| Relevance bridge | `research_area_primary`, optional `institution` | Use concise relevance and affiliation context. |
| CTA body | none | Keep business CTA copy fixed in the template, not database-driven. |
| Audit only | `similarity`, `email_validity`, `is_corresponding_author`, `author_order`, `times_cited` | Filter and audit with these fields; do not put them in prose. |

The exported columns, in order, are `recipient_name`, `email`, `anchor_title`, `anchor_source_title`, `anchor_year`, `research_area_primary`, `institution`, `country`, followed by the audit fields above. Sensitive or enrichment signals such as `ethnic_chinese`, QS, Fortune, and other ranking fields are deliberately absent.

The adapter groups source `records` by lowercased email and provides each group to the pure projection seam. It preserves multiple paper evidences so `EmailAnchorSelection` can choose one deterministic anchor; `MailSendGate` then decides whether that AuthorContact is exportable.
