from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator


SortOrder = Literal["asc", "desc"]
ExportFormat = Literal["csv", "xlsx"]


class SortItem(BaseModel):
    field: str
    order: SortOrder = "asc"


class QueryRequest(BaseModel):
    entity: str
    query_tree: Dict[str, Any]
    select_fields: List[str] = Field(default_factory=list)
    sort: List[SortItem] = Field(default_factory=list)
    page: int = 1
    page_size: int = 20
    search_after: Optional[List[Any]] = None

    @field_validator("page")
    @classmethod
    def validate_page(cls, value: int) -> int:
        if value < 1:
            raise ValueError("page must be >= 1")
        return value

    @field_validator("page_size")
    @classmethod
    def validate_page_size(cls, value: int) -> int:
        if value < 1 or value > 1000:
            raise ValueError("page_size must be between 1 and 1000")
        return value


class CountRequest(BaseModel):
    entity: str
    query_tree: Dict[str, Any]


class ExportRequest(BaseModel):
    entity: str
    query_tree: Dict[str, Any]
    select_fields: List[str] = Field(default_factory=list)
    sort: List[SortItem] = Field(default_factory=list)
    format: ExportFormat = "csv"

