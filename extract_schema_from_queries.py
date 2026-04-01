from __future__ import annotations
import argparse
import json
import re
from pathlib import Path

COLUMN_REF_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b")
FROM_CLAUSE_PATTERN = re.compile(
    r"\bFROM\b(.*?)(?=\bWHERE\b|\bGROUP\s+BY\b|\bORDER\s+BY\b|\bHAVING\b|\bLIMIT\b|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|;|$)",
    flags=re.IGNORECASE | re.DOTALL,
)
JOIN_SPLIT_PATTERN = re.compile(r"\b(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\b", flags=re.IGNORECASE)
ON_SPLIT_PATTERN = re.compile(r"\bON\b", flags=re.IGNORECASE)
TABLE_REF_PATTERN = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)?\s*$",
    flags=re.IGNORECASE,
)
GROUP_BY_CLAUSE_PATTERN = re.compile(
    r"\bGROUP\s+BY\b(.*?)(?=\bORDER\s+BY\b|\bHAVING\b|\bLIMIT\b|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|;|$)",
    flags=re.IGNORECASE | re.DOTALL,
)
SELECT_DISTINCT_PATTERN = re.compile(
    r"^\s*SELECT\s+DISTINCT\s+(.*?)\bFROM\b",
    flags=re.IGNORECASE | re.DOTALL,
)
PREDICATE_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|<>|!=|>=|<=|>|<|\bNOT\s+LIKE\b|\bLIKE\b|\bNOT\s+IN\b|\bIN\b|\bBETWEEN\b)\s*",
    flags=re.IGNORECASE,
)
RIGHT_COLUMN_PATTERN = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)")
EQUALITY_OPS = {"=", "<>", "!=", "LIKE", "NOT LIKE", "IN", "NOT IN"}
RANGE_OPS = {">", ">=", "<", "<=", "BETWEEN"}
CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+TABLE\s+((?:\"[^\"]+\")|(?:[A-Za-z_][A-Za-z0-9_]*))\s*\((.*?)\)\s*;",
    flags=re.IGNORECASE | re.DOTALL,
)
PRIMARY_KEY_TABLE_PATTERN = re.compile(r"PRIMARY\s+KEY\s*\((.*?)\)", flags=re.IGNORECASE)
TYPE_SPLIT_PATTERN = re.compile(
    r"\b(?:NOT\s+NULL|NULL|PRIMARY\s+KEY|REFERENCES|CHECK|DEFAULT)\b",
    flags=re.IGNORECASE,
)
JOIN_EQUALITY_PATTERN = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b",
    flags=re.IGNORECASE,
)
INDEX_DDL_PATTERN = re.compile(
    r"CREATE\s+INDEX\s+((?:\"[^\"]+\")|(?:[A-Za-z_][A-Za-z0-9_]*))\s+ON\s+((?:(?:\"[^\"]+\")|(?:[A-Za-z_][A-Za-z0-9_]*))(?:\.(?:(?:\"[^\"]+\")|(?:[A-Za-z_][A-Za-z0-9_]*)))?)\s*(?:USING\s+[A-Za-z_][A-Za-z0-9_]*\s*)?\(\s*((?:\"[^\"]+\")|(?:[A-Za-z_][A-Za-z0-9_]*))\s*\)\s*;",
    flags=re.IGNORECASE,
)
FK_PREFIX_SPECIAL_PK_TABLE = {
    "MOVIE": "TITLE",
    "LINKED_MOVIE": "TITLE",
    "PERSON": "NAME",
    "PERSON_ROLE": "CHAR_NAME",
    "ROLE": "ROLE_TYPE",
    "COMPANY": "COMPANY_NAME",
}


def _normalize(name: str) -> str:
    return name.strip().upper()


def _iter_query_strings(query_root: Path):
    for file_path in sorted(query_root.rglob("*")):
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in {".txt", ".sql"}:
            continue
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                if "#####" in line:
                    _, sql = line.split("#####", 1)
                    sql = sql.strip()
                    if sql:
                        yield sql
                elif line.lower().startswith("select"):
                    yield line


def _extract_alias_to_table(sql: str) -> dict[str, str]:
    alias_to_table: dict[str, str] = {}
    match = FROM_CLAUSE_PATTERN.search(sql)
    if not match:
        return alias_to_table
    from_clause = match.group(1)
    normalized_clause = JOIN_SPLIT_PATTERN.sub(",", from_clause)
    table_refs = [part.strip() for part in normalized_clause.split(",") if part.strip()]
    for ref in table_refs:
        ref = ON_SPLIT_PATTERN.split(ref, maxsplit=1)[0].strip()
        table_match = TABLE_REF_PATTERN.match(ref)
        if not table_match:
            continue
        table_name = table_match.group(1)
        alias = table_match.group(2) or table_name
        alias_to_table[_normalize(alias)] = _normalize(table_name)
    return alias_to_table


def _extract_used_table_columns(sql: str, alias_to_table: dict[str, str]) -> dict[str, set[str]]:
    used: dict[str, set[str]] = {table: set() for table in alias_to_table.values()}
    for alias, col in COLUMN_REF_PATTERN.findall(sql):
        alias_key = _normalize(alias)
        col_key = _normalize(col)
        table = alias_to_table.get(alias_key)
        if table:
            used.setdefault(table, set()).add(col_key)
    return used


def _split_sql_items(expr: str) -> list[str]:
    parts = []
    current = []
    depth = 0
    for ch in expr:
        if ch == "(":
            depth += 1
            current.append(ch)
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            current.append(ch)
            continue
        if ch == "," and depth == 0:
            item = "".join(current).strip()
            if item:
                parts.append(item)
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _map_sql_type_to_schema_type(raw_type: str) -> str:
    t = raw_type.strip().lower()
    if t.startswith("integer") or t.startswith("int") or t.startswith("smallint") or t.startswith("bigint"):
        return "int"
    if t.startswith("real") or t.startswith("double") or t.startswith("numeric") or t.startswith("decimal") or t.startswith("float"):
        return "float"
    if t.startswith("boolean"):
        return "boolean"
    if t.startswith("date") or t.startswith("timestamp"):
        return "date"
    return "str"


def _parse_column_definition(item: str) -> tuple[str, str, bool] | None:
    if re.match(r"^\s*PRIMARY\s+KEY\b", item, flags=re.IGNORECASE):
        return None
    m = re.match(r'^\s*("?[\w]+"?)\s+(.+?)\s*$', item, flags=re.DOTALL)
    if not m:
        return None
    column_name = _normalize(m.group(1).strip('"'))
    remainder = " ".join(m.group(2).replace("\n", " ").split())
    is_pk = bool(re.search(r"\bPRIMARY\s+KEY\b", remainder, flags=re.IGNORECASE))
    base_type = TYPE_SPLIT_PATTERN.split(remainder, maxsplit=1)[0].strip()
    if base_type == "":
        return None
    return column_name, _map_sql_type_to_schema_type(base_type), is_pk


def _parse_create_sql(create_sql_path: Path) -> tuple[dict[str, dict[str, str]], dict[str, list[str]]]:
    text = create_sql_path.read_text(encoding="utf-8", errors="ignore")
    relations: dict[str, dict[str, str]] = {}
    pk_columns: dict[str, list[str]] = {}
    for m in CREATE_TABLE_PATTERN.finditer(text):
        table = _normalize(m.group(1).strip('"'))
        body = m.group(2)
        columns: dict[str, str] = {}
        pks: list[str] = []
        for item in _split_sql_items(body):
            parsed = _parse_column_definition(item)
            if parsed is None:
                pk_match = PRIMARY_KEY_TABLE_PATTERN.search(item)
                if pk_match:
                    for pk_col in _split_sql_items(pk_match.group(1)):
                        c = _normalize(pk_col.strip().strip('"'))
                        if c not in pks:
                            pks.append(c)
                continue
            col, dtype, is_pk = parsed
            columns[col] = dtype
            if is_pk and col not in pks:
                pks.append(col)
        if columns:
            relations[table] = columns
            pk_columns[table] = pks
    if len(relations) == 0:
        raise ValueError(f"未在建表 SQL 中解析到 CREATE TABLE: {create_sql_path}")
    return relations, pk_columns


def _resolve_fk_pk_table(prefix: str, relations: dict[str, dict[str, str]], pk_columns: dict[str, list[str]]) -> str | None:
    prefix_key = _normalize(prefix)
    raw_candidates: list[str] = [prefix_key]
    if prefix_key.startswith("LINKED_"):
        raw_candidates.append(prefix_key[len("LINKED_") :])
    candidates: list[str] = []
    for token in raw_candidates:
        special = FK_PREFIX_SPECIAL_PK_TABLE.get(token)
        if special is not None and special not in candidates:
            candidates.append(special)
        if token not in candidates:
            candidates.append(token)
    for candidate in candidates:
        if candidate in relations and "ID" in pk_columns.get(candidate, []):
            return candidate
    return None


def _fk_column_priority(column_name: str) -> tuple[int, str]:
    col = _normalize(column_name)
    if col == "MOVIE_ID":
        return (0, col)
    if col == "LINKED_MOVIE_ID":
        return (1, col)
    if col == "PERSON_ID":
        return (2, col)
    return (3, col)


def _parse_fk_pk_joins_from_index_sql(
    index_sql_path: Path, relations: dict[str, dict[str, str]], pk_columns: dict[str, list[str]]
) -> dict[str, list[dict[str, str]]]:
    text = index_sql_path.read_text(encoding="utf-8", errors="ignore")
    fk_pk: dict[str, list[dict[str, str]]] = {}
    for match in INDEX_DDL_PATTERN.finditer(text):
        table = _normalize(match.group(2).split(".")[-1].strip('"'))
        col = _normalize(match.group(3).strip('"'))
        if table not in relations:
            continue
        if col not in relations[table]:
            continue
        if col == "ID" or not col.endswith("_ID"):
            continue
        prefix = col[: -len("_ID")]
        pk_table = _resolve_fk_pk_table(prefix, relations, pk_columns)
        if pk_table is None or pk_table == table:
            continue
        fk_pk.setdefault(table, [])
        item = {"fk": col, "pk_relation": pk_table, "pk": "ID"}
        if item not in fk_pk[table]:
            fk_pk[table].append(item)
    for table, items in fk_pk.items():
        items.sort(key=lambda item: _fk_column_priority(item["fk"]))
    if len(fk_pk) == 0:
        raise ValueError(f"未在索引 SQL 中解析到 fk_pk_joins_dict: {index_sql_path}")
    return fk_pk


def _split_columns(expr: str) -> list[str]:
    return [part.strip() for part in expr.split(",") if part.strip()]


def _extract_groupby_for_query(sql: str, alias_to_table: dict[str, str]) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    match = GROUP_BY_CLAUSE_PATTERN.search(sql)
    if match:
        for item in _split_columns(match.group(1)):
            for alias, col in COLUMN_REF_PATTERN.findall(item):
                alias_key = _normalize(alias)
                table = alias_to_table.get(alias_key)
                if table:
                    grouped.setdefault(table, set()).add(_normalize(col))
    distinct_match = SELECT_DISTINCT_PATTERN.search(sql)
    if distinct_match:
        for item in _split_columns(distinct_match.group(1)):
            for alias, col in COLUMN_REF_PATTERN.findall(item):
                alias_key = _normalize(alias)
                table = alias_to_table.get(alias_key)
                if table:
                    grouped.setdefault(table, set()).add(_normalize(col))
    return grouped


def _extract_predicates_for_query(sql: str, alias_to_table: dict[str, str]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    equality_vars: dict[str, set[str]] = {}
    range_vars: dict[str, set[str]] = {}
    for match in PREDICATE_PATTERN.finditer(sql):
        alias = _normalize(match.group(1))
        col = _normalize(match.group(2))
        op = re.sub(r"\s+", " ", match.group(3).upper())
        table = alias_to_table.get(alias)
        if not table:
            continue
        rhs = sql[match.end() :]
        right_col_match = RIGHT_COLUMN_PATTERN.match(rhs)
        if op == "=" and right_col_match:
            right_alias = _normalize(right_col_match.group(1))
            if right_alias in alias_to_table:
                continue
        if op in EQUALITY_OPS:
            equality_vars.setdefault(table, set()).add(col)
        elif op in RANGE_OPS:
            range_vars.setdefault(table, set()).add(col)
    return equality_vars, range_vars


def _extract_join_variables_for_query(sql: str, alias_to_table: dict[str, str]) -> dict[str, set[str]]:
    join_variables: dict[str, set[str]] = {}
    for m in JOIN_EQUALITY_PATTERN.finditer(sql):
        left_alias = _normalize(m.group(1))
        left_col = _normalize(m.group(2))
        right_alias = _normalize(m.group(3))
        right_col = _normalize(m.group(4))
        left_table = alias_to_table.get(left_alias)
        right_table = alias_to_table.get(right_alias)
        if left_table is not None:
            join_variables.setdefault(left_table, set()).add(left_col)
        if right_table is not None:
            join_variables.setdefault(right_table, set()).add(right_col)
    return join_variables


def _to_sorted_map(items: dict[str, set[str]]) -> dict[str, list[str]]:
    return {table: sorted(cols) for table, cols in sorted(items.items()) if cols}


def _to_relation_ordered_map(items: dict[str, set[str]], relations: dict[str, dict[str, str]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for table, columns in relations.items():
        chosen = items.get(table, set())
        if len(chosen) == 0:
            continue
        ordered = [col for col in columns.keys() if col in chosen]
        if len(ordered) > 0:
            result[table] = ordered
    return result


def extract_schema_from_queries(
    query_root: Path, create_sql_path: Path, index_sql_path: Path, output_path: Path, schema_name: str
):
    relations, pk_columns = _parse_create_sql(create_sql_path)
    fk_pk_joins_dict = _parse_fk_pk_joins_from_index_sql(index_sql_path, relations, pk_columns)
    groupby_candidates: dict[str, set[tuple[str, ...]]] = {}
    join_variables: dict[str, set[str]] = {table: set(pk_columns.get(table, [])) for table in relations.keys()}
    for table, columns in relations.items():
        if "KIND_ID" in columns:
            join_variables.setdefault(table, set()).add("KIND_ID")
    for table, items in fk_pk_joins_dict.items():
        for item in items:
            join_variables.setdefault(table, set()).add(item["fk"])
            join_variables.setdefault(item["pk_relation"], set()).add(item["pk"])
    equality_predicates: dict[str, set[str]] = {}
    range_predicates: dict[str, set[str]] = {}
    query_count = 0
    for sql in _iter_query_strings(query_root):
        query_count += 1
        alias_to_table = _extract_alias_to_table(sql)
        if not alias_to_table:
            continue
        _extract_used_table_columns(sql, alias_to_table)
        joined = _extract_join_variables_for_query(sql, alias_to_table)
        for table, cols in joined.items():
            if table in relations:
                join_variables.setdefault(table, set()).update(cols)
        grouped = _extract_groupby_for_query(sql, alias_to_table)
        for table, cols in grouped.items():
            if table not in relations or not cols:
                continue
            groupby_candidates.setdefault(table, set()).add(tuple(sorted(cols)))
        eq_vars, rg_vars = _extract_predicates_for_query(sql, alias_to_table)
        for table, cols in eq_vars.items():
            if table in relations:
                equality_predicates.setdefault(table, set()).update(cols)
        for table, cols in rg_vars.items():
            if table in relations:
                range_predicates.setdefault(table, set()).update(cols)
    if query_count == 0:
        raise ValueError(f"未在目录中解析到有效查询: {query_root}")
    groupby_variables = {}
    for table, combos in sorted(groupby_candidates.items()):
        if not combos:
            continue
        groupby_variables[table] = [list(combo) for combo in sorted(combos)]
    filtered_schema = {
        "name": schema_name,
        "relations": relations,
        "join_variables": _to_relation_ordered_map(join_variables, relations),
        "groupby_variables": groupby_variables,
        "equality_predicate_variables": _to_sorted_map(equality_predicates),
        "range_predicate_variables": _to_sorted_map(range_predicates),
        "fk_pk_joins_dict": fk_pk_joins_dict,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(filtered_schema, f, indent=2, ensure_ascii=False)
    print(f"解析查询条数: {query_count}")
    print(f"groupby 涉及表数量: {len(filtered_schema['groupby_variables'])}")
    print(f"等值谓词涉及表数量: {len(filtered_schema['equality_predicate_variables'])}")
    print(f"范围谓词涉及表数量: {len(filtered_schema['range_predicate_variables'])}")
    print(f"输出路径: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="从查询集合提取最小化 schema JSON。")
    parser.add_argument(
        "--query-root",
        type=Path,
        default=Path("/data1/chenjunming/projects/QOClassifier/LpBound/data/datasets/IMDB_DATA_DRIFT"),
    )
    parser.add_argument("--create-sql", type=Path, required=True)
    parser.add_argument("--index-sql", type=Path, required=True)
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("/data1/chenjunming/projects/QOClassifier/LpBound/data/datasets/IMDB_DATA_DRIFT/imdb_schema_extracted.json"),
    )
    args = parser.parse_args()
    extract_schema_from_queries(args.query_root, args.create_sql, args.index_sql, args.output, args.name)


if __name__ == "__main__":
    main()
