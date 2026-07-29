"""Pure MailMergeProjection seam for AuthorContact outreach."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from typing import Any, Iterable, Mapping

FAILED_VALIDITY_TOKENS = {
    "failed", "fail", "invalid", "false", "0", "syntax_error", "mx_fail",
    "smtp_fail", "bounce",
}
PROJECTION_COLUMNS = (
    "recipient_name", "email", "anchor_title", "anchor_source_title", "anchor_year",
    "research_area_primary", "institution", "country", "similarity", "email_validity",
    "is_corresponding_author", "author_order", "times_cited",
)


@dataclass(frozen=True)
class PaperEvidence:
    """A publication candidate from which one email anchor is selected."""

    title: str | None = None
    source_title: str | None = None
    publication_year: int | None = None
    author_order: int | None = None
    is_corresponding_author: bool = False
    times_cited: int | None = None
    similarity: float | None = None


@dataclass(frozen=True)
class AuthorContactCandidate:
    """An AuthorContact and its candidate EmailAnchorPublication evidence."""

    email: str | None = None
    short_name: str | None = None
    full_name: str | None = None
    country: str | None = None
    institution: str | None = None
    institution_norm: str | None = None
    research_areas: str | list[str] | None = None
    wos_categories: str | list[str] | None = None
    similarity: float | None = None
    email_validity: str | bool | int | None = None
    paper_evidences: list[PaperEvidence | Mapping[str, Any]] = field(default_factory=list)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if is_dataclass(value):
        return asdict(value)
    raise TypeError("candidates and evidences must be mappings or dataclasses")


def _non_empty(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip() or None
    return value


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(float(value)) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "t"}
    return bool(value)


def _first_token(value: Any) -> str | None:
    if isinstance(value, str):
        values = value.replace("|", ";").split(";")
    elif isinstance(value, Iterable) and not isinstance(value, (bytes, Mapping)):
        values = value
    else:
        values = ()
    for item in values:
        cleaned = _non_empty(item)
        if cleaned is not None:
            return str(cleaned)
    return None


def _evidences(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("paper_evidences", "evidences", "publications", "papers"):
        values = candidate.get(key)
        if values is not None:
            return [_mapping(value) for value in values]
    # A flat candidate is also a single evidence, useful for adapters and callers.
    return [candidate]


def _coalesce_similarity(evidence: Mapping[str, Any], fallback_similarity: Any) -> Any:
    value = evidence.get("similarity")
    if value is not None and value != "":
        return value
    return fallback_similarity


def _select_anchor(evidences: list[dict[str, Any]], fallback_similarity: Any) -> dict[str, Any] | None:
    if not evidences:
        return None

    def ranking(item: tuple[int, dict[str, Any]]) -> tuple[float, int, int, int, float, int]:
        index, evidence = item
        author_order = _integer(evidence.get("author_order"), default=10**9)
        return (
            -int(_truthy(evidence.get("is_corresponding_author"))),
            author_order,
            -_integer(evidence.get("publication_year")),
            -_integer(evidence.get("times_cited")),
            -_number(_coalesce_similarity(evidence, fallback_similarity)),
            index,
        )

    return min(enumerate(evidences), key=ranking)[1]


def _is_failed_validity(value: Any) -> bool:
    return str(value).strip().lower() in FAILED_VALIDITY_TOKENS


def build_mail_merge_projection(
    candidates: Iterable[AuthorContactCandidate | Mapping[str, Any]], *, similarity_threshold: float = 0.6
) -> list[dict[str, Any]]:
    """Select, gate, and flatten one EmailAnchorPublication per AuthorContact.

    This function performs no I/O and intentionally omits sensitive/ranking enrichment
    fields outside the documented MailMergeProjection contract.
    """
    rows: list[dict[str, Any]] = []
    for raw_candidate in candidates:
        candidate = _mapping(raw_candidate)
        anchor = _select_anchor(_evidences(candidate), candidate.get("similarity"))
        if anchor is None:
            continue
        recipient_name = _non_empty(candidate.get("full_name")) or _non_empty(candidate.get("short_name"))
        email = _non_empty(candidate.get("email"))
        anchor_title = _non_empty(anchor.get("title", anchor.get("anchor_title")))
        similarity = _number(_coalesce_similarity(anchor, candidate.get("similarity")))
        email_validity = candidate.get("email_validity", anchor.get("email_validity"))
        if not recipient_name or not email or not anchor_title:
            continue
        if _is_failed_validity(email_validity) or similarity < similarity_threshold:
            continue
        rows.append({
            "recipient_name": recipient_name,
            "email": email,
            "anchor_title": anchor_title,
            "anchor_source_title": _non_empty(anchor.get("source_title", anchor.get("anchor_source_title"))),
            "anchor_year": anchor.get("publication_year", anchor.get("anchor_year")),
            "research_area_primary": _first_token(candidate.get("research_areas"))
            or _first_token(candidate.get("wos_categories")),
            "institution": _non_empty(candidate.get("institution_norm"))
            or _non_empty(candidate.get("institution")),
            "country": _non_empty(candidate.get("country")),
            "similarity": similarity,
            "email_validity": email_validity,
            "is_corresponding_author": _truthy(anchor.get("is_corresponding_author")),
            "author_order": anchor.get("author_order"),
            "times_cited": anchor.get("times_cited"),
        })
    return rows
