from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from search_app.field_registry import ENTITY_UI_CONFIG, resolve_cn_label


ROOT_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT_DIR / "schema_v1.yaml"
MAPPING_PATH = ROOT_DIR / "opensearch_mapping_v1.json"


@dataclass(frozen=True)
class FieldMeta:
    entity_key: str
    index_name: str
    field_path: str
    cn_label: str
    field_type: str
    required: bool
    description: str
    is_array: bool
    is_nested: bool
    nested_path: Optional[str]
    operators: List[str]
    sortable: bool
    aggregatable: bool
    default_visible: bool
    table_renderer: str
    dsl_strategy: str
    sql_expr: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EntityMeta:
    entity_key: str
    index_name: str
    cn_label: str
    description: str
    primary_key: str
    default_visible_fields: List[str]
    default_sort: List[dict]
    fields: List[FieldMeta] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["field_count"] = len(self.fields)
        return data


TYPE_OPERATOR_MAP: Dict[str, List[str]] = {
    "keyword": ["eq", "neq", "in", "not_in", "exists", "not_exists", "prefix"],
    "text": ["match", "match_phrase", "exists", "not_exists"],
    "integer": ["eq", "gt", "gte", "lt", "lte", "between", "exists"],
    "float": ["eq", "gt", "gte", "lt", "lte", "between", "exists"],
    "date": ["eq", "gt", "gte", "lt", "lte", "between", "exists"],
    "boolean": ["eq"],
    "nested": ["exists"],
}


SORTABLE_TYPES = {"keyword", "integer", "float", "date", "boolean"}
AGGREGATABLE_TYPES = {"keyword", "integer", "float", "date", "boolean"}
DATE_TYPES = {"date", "datetime"}
NUMERIC_TYPES = {"integer", "float", "long", "double"}


def normalize_field_type(mapping_type: str, schema_type: Optional[str]) -> str:
    if mapping_type == "date" or schema_type in DATE_TYPES:
        return "date"
    if mapping_type in NUMERIC_TYPES:
        return "float" if mapping_type in {"float", "double"} else "integer"
    if mapping_type in {"keyword", "text", "boolean", "nested"}:
        return mapping_type
    if schema_type:
        base = schema_type.replace("[]", "")
        if base == "datetime":
            return "date"
        if base in TYPE_OPERATOR_MAP:
            return base
    return mapping_type


def infer_table_renderer(field_type: str, is_array: bool, is_nested: bool) -> str:
    if is_nested:
        return "nested"
    if is_array:
        return "array"
    if field_type == "boolean":
        return "boolean"
    if field_type == "date":
        return "datetime"
    if field_type in {"integer", "float"}:
        return "number"
    return "text"


def infer_dsl_strategy(field_type: str, is_nested: bool) -> str:
    if is_nested:
        return "nested"
    if field_type == "text":
        return "match/match_phrase"
    if field_type in {"integer", "float", "date"}:
        return "range"
    if field_type == "boolean":
        return "term"
    return "term/terms"


def infer_sql_expr(field_path: str, field_type: str, is_nested: bool) -> str:
    if is_nested:
        return "dsl_filter_only"
    if field_type == "text":
        return "dsl_filter_only"
    return field_path


def schema_field_lookup(entity_fields: Iterable[dict]) -> Dict[str, dict]:
    return {field_info["name"]: field_info for field_info in entity_fields}


def build_field_meta(
    entity_key: str,
    index_name: str,
    field_path: str,
    mapping_definition: dict,
    schema_definition: Optional[dict],
    default_visible_fields: List[str],
    nested_path: Optional[str] = None,
) -> FieldMeta:
    schema_type = schema_definition.get("type") if schema_definition else None
    mapping_type = mapping_definition.get("type", "object")
    normalized_type = normalize_field_type(mapping_type, schema_type)
    is_array = bool(schema_type and schema_type.endswith("[]"))
    is_nested = nested_path is not None
    required = bool(schema_definition and schema_definition.get("required"))
    description = schema_definition.get("description", "") if schema_definition else ""
    operators = TYPE_OPERATOR_MAP.get(normalized_type, ["exists"])
    sortable = normalized_type in SORTABLE_TYPES and not is_nested
    aggregatable = normalized_type in AGGREGATABLE_TYPES and not is_nested

    return FieldMeta(
        entity_key=entity_key,
        index_name=index_name,
        field_path=field_path,
        cn_label=resolve_cn_label(field_path),
        field_type=normalized_type,
        required=required,
        description=description,
        is_array=is_array,
        is_nested=is_nested,
        nested_path=nested_path,
        operators=operators,
        sortable=sortable,
        aggregatable=aggregatable,
        default_visible=field_path in default_visible_fields,
        table_renderer=infer_table_renderer(normalized_type, is_array, is_nested),
        dsl_strategy=infer_dsl_strategy(normalized_type, is_nested),
        sql_expr=infer_sql_expr(field_path, normalized_type, is_nested),
    )


class SearchCatalog:
    def __init__(self, schema_path: Path = SCHEMA_PATH, mapping_path: Path = MAPPING_PATH) -> None:
        self.schema_path = schema_path
        self.mapping_path = mapping_path
        self.entities = self._load_entities()

    def _load_entities(self) -> Dict[str, EntityMeta]:
        schema = yaml.safe_load(self.schema_path.read_text(encoding="utf-8"))
        mapping = json.loads(self.mapping_path.read_text(encoding="utf-8"))

        entity_catalog: Dict[str, EntityMeta] = {}
        schema_entities: Dict[str, dict] = schema.get("entities", {})
        mapping_indices: Dict[str, dict] = mapping.get("indices", {})

        for entity_key, entity_schema in schema_entities.items():
            index_name = f"{entity_key}_v1"
            if index_name not in mapping_indices:
                raise KeyError(f"Index {index_name} not found in mapping file.")

            ui_config = ENTITY_UI_CONFIG[entity_key]
            props = mapping_indices[index_name]["mappings"]["properties"]
            schema_fields = schema_field_lookup(entity_schema.get("fields", []))
            fields: List[FieldMeta] = []

            for field_name, mapping_definition in props.items():
                schema_definition = schema_fields.get(field_name)
                fields.append(
                    build_field_meta(
                        entity_key=entity_key,
                        index_name=index_name,
                        field_path=field_name,
                        mapping_definition=mapping_definition,
                        schema_definition=schema_definition,
                        default_visible_fields=ui_config.default_visible,
                    )
                )

                if mapping_definition.get("type") == "nested":
                    nested_props = mapping_definition.get("properties", {})
                    for nested_field_name, nested_definition in nested_props.items():
                        nested_field_path = f"{field_name}.{nested_field_name}"
                        fields.append(
                            build_field_meta(
                                entity_key=entity_key,
                                index_name=index_name,
                                field_path=nested_field_path,
                                mapping_definition=nested_definition,
                                schema_definition=None,
                                default_visible_fields=ui_config.default_visible,
                                nested_path=field_name,
                            )
                        )

            entity_catalog[entity_key] = EntityMeta(
                entity_key=entity_key,
                index_name=index_name,
                cn_label=ui_config.title,
                description=entity_schema.get("description", ""),
                primary_key=entity_schema.get("primary_key", ""),
                default_visible_fields=ui_config.default_visible,
                default_sort=ui_config.default_sort,
                fields=sorted(fields, key=lambda item: item.field_path),
            )

        return entity_catalog

    def list_entities(self) -> List[Dict[str, Any]]:
        entities = []
        for entity_meta in self.entities.values():
            entities.append(
                {
                    "entity_key": entity_meta.entity_key,
                    "index_name": entity_meta.index_name,
                    "cn_label": entity_meta.cn_label,
                    "description": entity_meta.description,
                    "primary_key": entity_meta.primary_key,
                    "field_count": len(entity_meta.fields),
                    "default_visible_fields": entity_meta.default_visible_fields,
                    "default_sort": entity_meta.default_sort,
                }
            )
        return entities

    def get_entity(self, entity_key: str) -> EntityMeta:
        if entity_key not in self.entities:
            raise KeyError(f"Unsupported entity: {entity_key}")
        return self.entities[entity_key]

    def get_field(self, entity_key: str, field_path: str) -> FieldMeta:
        entity_meta = self.get_entity(entity_key)
        for field_meta in entity_meta.fields:
            if field_meta.field_path == field_path:
                return field_meta
        raise KeyError(f"Unsupported field for {entity_key}: {field_path}")

