from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from search_app.catalog import FieldMeta, SearchCatalog


SUPPORTED_LOGIC = {"and", "or"}
NEGATIVE_OPERATORS = {"neq", "not_in", "not_exists"}
DEEP_PAGINATION_THRESHOLD = 10000


@dataclass
class QueryBuildResult:
    entity: str
    index_name: str
    select_fields: List[str]
    sort: List[dict]
    page: int
    page_size: int
    warnings: List[str] = field(default_factory=list)
    query: Dict[str, Any] = field(default_factory=dict)
    body: Dict[str, Any] = field(default_factory=dict)
    sql_mode: str = "sql"
    sql: str = ""
    sql_request_body: Dict[str, Any] = field(default_factory=dict)


def is_group_node(node: Dict[str, Any]) -> bool:
    return node.get("type", "group") == "group"


def is_rule_node(node: Dict[str, Any]) -> bool:
    return node.get("type") == "rule"


def prune_query_node(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if is_group_node(node):
        children = []
        for child in node.get("children", []):
            normalized_child = prune_query_node(child)
            if normalized_child is not None:
                children.append(normalized_child)
        return {
            "type": "group",
            "logic": str(node.get("logic", "and")).lower(),
            "children": children,
        }

    if is_rule_node(node):
        field_name = str(node.get("field", "")).strip()
        operator = str(node.get("operator", "")).strip()
        if not field_name and not operator:
            return None
        if not field_name or not operator:
            raise ValueError("rule.field and rule.operator are required")
        normalized_rule = dict(node)
        normalized_rule["field"] = field_name
        normalized_rule["operator"] = operator
        return normalized_rule

    raise ValueError("Unknown query node type")


def build_exists_query(field_path: str) -> Dict[str, Any]:
    return {"exists": {"field": field_path}}


def build_simple_rule_query(field_meta: FieldMeta, operator: str, rule: Dict[str, Any]) -> Dict[str, Any]:
    field_path = field_meta.field_path
    value = rule.get("value")
    values = rule.get("values", [])
    range_value = rule.get("range", {})

    if operator == "eq":
        return {"term": {field_path: value}}
    if operator == "neq":
        return {"bool": {"must_not": [{"term": {field_path: value}}]}}
    if operator == "in":
        return {"terms": {field_path: values}}
    if operator == "not_in":
        return {"bool": {"must_not": [{"terms": {field_path: values}}]}}
    if operator == "prefix":
        return {"prefix": {field_path: value}}
    if operator == "match":
        return {"match": {field_path: value}}
    if operator == "match_phrase":
        return {"match_phrase": {field_path: value}}
    if operator == "exists":
        return build_exists_query(field_path)
    if operator == "not_exists":
        return {"bool": {"must_not": [build_exists_query(field_path)]}}
    if operator in {"gt", "gte", "lt", "lte"}:
        return {"range": {field_path: {operator: value}}}
    if operator == "between":
        payload: Dict[str, Any] = {}
        if range_value.get("from") is not None:
            payload["gte"] = range_value["from"]
        if range_value.get("to") is not None:
            payload["lte"] = range_value["to"]
        return {"range": {field_path: payload}}

    raise ValueError(f"Unsupported operator {operator} for field {field_path}")


def group_rule_children(children: Sequence[Dict[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    nested_groups: Dict[str, List[Dict[str, Any]]] = {}
    plain_children: List[Dict[str, Any]] = []
    for child in children:
        if is_rule_node(child) and child.get("nested_path"):
            nested_groups.setdefault(child["nested_path"], []).append(child)
        else:
            plain_children.append(child)
    return nested_groups, plain_children


def build_group_query(
    group: Dict[str, Any],
    entity_key: str,
    catalog: SearchCatalog,
    warnings: List[str],
    regroup_nested_rules: bool = True,
) -> Dict[str, Any]:
    logic = str(group.get("logic", "and")).lower()
    if logic not in SUPPORTED_LOGIC:
        raise ValueError(f"Unsupported group logic: {logic}")

    children = group.get("children", [])
    if not isinstance(children, list):
        raise ValueError("group.children must be a list")

    if regroup_nested_rules:
        nested_children, plain_children = group_rule_children(children)
    else:
        nested_children, plain_children = {}, children
    clauses: List[Dict[str, Any]] = []

    for child in plain_children:
        clauses.append(build_node_query(child, entity_key, catalog, warnings))

    for nested_path, nested_rules in nested_children.items():
        nested_query = build_group_query(
            {"type": "group", "logic": logic, "children": nested_rules},
            entity_key=entity_key,
            catalog=catalog,
            warnings=warnings,
            regroup_nested_rules=False,
        )
        clauses.append({"nested": {"path": nested_path, "query": nested_query}})

    if not clauses:
        return {"match_all": {}}
    if len(clauses) == 1:
        return clauses[0]
    if logic == "and":
        return {"bool": {"must": clauses}}
    return {"bool": {"should": clauses, "minimum_should_match": 1}}


def build_rule_query(rule: Dict[str, Any], entity_key: str, catalog: SearchCatalog, warnings: List[str]) -> Dict[str, Any]:
    field_name = rule.get("field")
    operator = rule.get("operator")
    if not field_name or not operator:
        raise ValueError("rule.field and rule.operator are required")

    field_meta = catalog.get_field(entity_key, field_name)
    if operator not in field_meta.operators:
        raise ValueError(f"Operator {operator} is not allowed for field {field_name}")

    if field_meta.field_type == "text" and operator in {"eq", "in", "prefix"}:
        raise ValueError(f"Text field {field_name} does not support exact filter operator {operator}")

    if operator in {"in", "not_in"} and not rule.get("values"):
        raise ValueError(f"Operator {operator} requires non-empty values")

    if operator == "between" and not isinstance(rule.get("range"), dict):
        raise ValueError("Operator between requires a range object")

    if field_meta.is_nested and rule.get("nested_path") != field_meta.nested_path:
        warnings.append(f"字段 {field_name} 的 nested_path 已自动修正为 {field_meta.nested_path}")

    return build_simple_rule_query(field_meta, operator, rule)


def build_node_query(node: Dict[str, Any], entity_key: str, catalog: SearchCatalog, warnings: List[str]) -> Dict[str, Any]:
    if is_group_node(node):
        return build_group_query(node, entity_key, catalog, warnings)
    if is_rule_node(node):
        return build_rule_query(node, entity_key, catalog, warnings)
    raise ValueError("Unknown query node type")


def default_sort_for_entity(entity_key: str, catalog: SearchCatalog) -> List[dict]:
    entity_meta = catalog.get_entity(entity_key)
    return list(entity_meta.default_sort)


def normalize_sort(
    entity_key: str,
    select_fields: Sequence[str],
    sort_items: Sequence[dict],
    catalog: SearchCatalog,
    warnings: List[str],
) -> List[dict]:
    effective_sort = list(sort_items) if sort_items else default_sort_for_entity(entity_key, catalog)
    normalized: List[dict] = []
    for sort_item in effective_sort:
        field_meta = catalog.get_field(entity_key, sort_item["field"])
        if not field_meta.sortable:
            warnings.append(f"字段 {field_meta.field_path} 不支持排序，已忽略。")
            continue
        normalized.append(
            {
                field_meta.field_path: {
                    "order": sort_item.get("order", "asc"),
                    "missing": "_last",
                }
            }
        )

    if not normalized:
        entity_meta = catalog.get_entity(entity_key)
        normalized = [
            {
                entity_meta.primary_key: {
                    "order": "asc",
                    "missing": "_last",
                }
            }
        ]

    tie_breaker_field = catalog.get_entity(entity_key).primary_key
    if not any(tie_breaker_field in item for item in normalized):
        normalized.append({tie_breaker_field: {"order": "asc", "missing": "_last"}})
    return normalized


def normalize_select_fields(entity_key: str, requested_fields: Sequence[str], catalog: SearchCatalog) -> List[str]:
    entity_meta = catalog.get_entity(entity_key)
    if not requested_fields:
        return entity_meta.default_visible_fields

    normalized: List[str] = []
    for field_name in requested_fields:
        catalog.get_field(entity_key, field_name)
        normalized.append(field_name)
    return normalized


def flatten_sql_candidates(node: Dict[str, Any], entity_key: str, catalog: SearchCatalog) -> Optional[List[str]]:
    if is_rule_node(node):
        field_meta = catalog.get_field(entity_key, node["field"])
        if field_meta.is_nested or field_meta.field_type == "text":
            return None

        operator = node["operator"]
        field_name = field_meta.sql_expr
        if field_name == "dsl_filter_only":
            return None

        if operator == "eq":
            return [f"{field_name} = {sql_literal(node.get('value'))}"]
        if operator == "neq":
            return [f"{field_name} <> {sql_literal(node.get('value'))}"]
        if operator == "in":
            return [f"{field_name} IN ({', '.join(sql_literal(item) for item in node.get('values', []))})"]
        if operator == "not_in":
            return [f"{field_name} NOT IN ({', '.join(sql_literal(item) for item in node.get('values', []))})"]
        if operator == "exists":
            return [f"{field_name} IS NOT NULL"]
        if operator == "not_exists":
            return [f"{field_name} IS NULL"]
        if operator in {"gt", "gte", "lt", "lte"}:
            sql_operator = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[operator]
            return [f"{field_name} {sql_operator} {sql_literal(node.get('value'))}"]
        if operator == "between":
            range_value = node.get("range", {})
            parts: List[str] = []
            if range_value.get("from") is not None:
                parts.append(f"{field_name} >= {sql_literal(range_value['from'])}")
            if range_value.get("to") is not None:
                parts.append(f"{field_name} <= {sql_literal(range_value['to'])}")
            return parts
        return None

    if not is_group_node(node):
        return None

    logic = str(node.get("logic", "and")).lower()
    if logic != "and":
        return None

    fragments: List[str] = []
    for child in node.get("children", []):
        child_fragments = flatten_sql_candidates(child, entity_key, catalog)
        if child_fragments is None:
            return None
        fragments.extend(child_fragments)
    return fragments


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def build_sql_preview(
    entity_key: str,
    index_name: str,
    select_fields: Sequence[str],
    sort_items: Sequence[dict],
    page: int,
    page_size: int,
    query_tree: Dict[str, Any],
    catalog: SearchCatalog,
    dsl_filter: Dict[str, Any],
) -> Tuple[str, str, Dict[str, Any]]:
    simple_where_parts = flatten_sql_candidates(query_tree, entity_key, catalog)
    select_expr = ", ".join(select_fields) if select_fields else "*"

    order_clause = ""
    if sort_items:
        order_parts = []
        for item in sort_items:
            field_name, payload = next(iter(item.items()))
            order_parts.append(f"{field_name} {payload.get('order', 'asc').upper()}")
        order_clause = " ORDER BY " + ", ".join(order_parts)

    offset = (page - 1) * page_size
    limit_clause = f" LIMIT {offset}, {page_size}"

    if simple_where_parts is not None:
        where_clause = ""
        if simple_where_parts:
            where_clause = " WHERE " + " AND ".join(simple_where_parts)
        sql = f"SELECT {select_expr} FROM {index_name}{where_clause}{order_clause}{limit_clause}"
        return "sql", sql, {"query": sql}

    sql = f"SELECT {select_expr} FROM {index_name}{order_clause}{limit_clause}"
    return "sql+filter", sql, {"query": sql, "filter": dsl_filter}


def build_search_request(
    *,
    entity_key: str,
    query_tree: Dict[str, Any],
    select_fields: Sequence[str],
    sort: Sequence[dict],
    page: int,
    page_size: int,
    search_after: Optional[List[Any]],
    catalog: SearchCatalog,
) -> QueryBuildResult:
    entity_meta = catalog.get_entity(entity_key)
    warnings: List[str] = []
    normalized_select_fields = normalize_select_fields(entity_key, select_fields, catalog)
    normalized_sort = normalize_sort(entity_key, normalized_select_fields, sort, catalog, warnings)
    normalized_query_tree = prune_query_node(query_tree) or {"type": "group", "logic": "and", "children": []}
    query = build_node_query(normalized_query_tree, entity_key, catalog, warnings)

    body: Dict[str, Any] = {
        "query": query,
        "_source": normalized_select_fields,
        "sort": normalized_sort,
    }

    if search_after:
        body["size"] = page_size
        body["search_after"] = search_after
    else:
        offset = (page - 1) * page_size
        if offset >= DEEP_PAGINATION_THRESHOLD:
            warnings.append(
                f"当前页偏移量 {offset} 超过 {DEEP_PAGINATION_THRESHOLD}，建议改用 search_after。"
            )
        body["from"] = offset
        body["size"] = page_size

    sql_mode, sql, sql_request_body = build_sql_preview(
        entity_key=entity_key,
        index_name=entity_meta.index_name,
        select_fields=normalized_select_fields,
        sort_items=normalized_sort,
        page=page,
        page_size=page_size,
        query_tree=normalized_query_tree,
        catalog=catalog,
        dsl_filter=query,
    )

    return QueryBuildResult(
        entity=entity_key,
        index_name=entity_meta.index_name,
        select_fields=normalized_select_fields,
        sort=normalized_sort,
        page=page,
        page_size=page_size,
        warnings=warnings,
        query=query,
        body=body,
        sql_mode=sql_mode,
        sql=sql,
        sql_request_body=sql_request_body,
    )


def pretty_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)
