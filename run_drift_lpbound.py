import argparse
import json
import os
import sys
import glob
import re
import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), "src"))
from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.paths import LpBoundPaths
from lpbound.acyclic.lpbound import build_lpbound_statistics, estimate

def remove_unsupported_predicates(sql):
    q = re.sub(r"SELECT.*?FROM", "SELECT COUNT(*) FROM", sql, flags=re.IGNORECASE | re.DOTALL).strip()
    if " WHERE " not in q.upper():
        return q
    def is_supported_predicate(term):
        t = term.strip()
        if t == "":
            return False
        up = t.upper()
        if " OR " in up or " LIKE " in up or " NOT LIKE " in up or " IN " in up or "!=" in up or "<>" in up:
            return False
        join_eq = re.match(
            r"^[A-Za-z_][\w]*\.[A-Za-z_][\w]*\s*=\s*[A-Za-z_][\w]*\.[A-Za-z_][\w]*$",
            t,
            flags=re.IGNORECASE,
        )
        if join_eq:
            return True
        eq_const = re.match(
            r"^[A-Za-z_][\w]*\.[A-Za-z_][\w]*\s*=\s*(?:'[^']*'|-?\d+(?:\.\d+)?)$",
            t,
            flags=re.IGNORECASE,
        )
        if eq_const:
            return True
        range_const = re.match(
            r"^[A-Za-z_][\w]*\.[A-Za-z_][\w]*\s*(?:>=|>|<=|<)\s*(?:-?\d+(?:\.\d+)?|'[^']*')$",
            t,
            flags=re.IGNORECASE,
        )
        if range_const:
            return True
        return False

    where_idx = re.search(r"\bWHERE\b", q, flags=re.IGNORECASE).start()
    head = q[:where_idx]
    cond = q[where_idx + 5 :].strip().rstrip(";")
    cond = re.sub(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\s+IN\s*\([^)]*\)", " ", cond, flags=re.IGNORECASE)
    cond = re.sub(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\s+NOT\s+LIKE\s+'[^']*'", " ", cond, flags=re.IGNORECASE)
    cond = re.sub(r"[A-Za-z_][\w]*\.[A-Za-z_][\w]*\s+LIKE\s+'[^']*'", " ", cond, flags=re.IGNORECASE)
    cond = re.sub(r"\([^)]*\s+OR\s+[^)]*\)", " ", cond, flags=re.IGNORECASE)
    parts = [p.strip() for p in re.split(r"\s+AND\s+", cond, flags=re.IGNORECASE)]
    kept = [p for p in parts if is_supported_predicate(p)]
    if len(kept) == 0:
        return head.strip() + ";"
    return f"{head} WHERE " + " AND ".join(kept) + ";"

def maybe_rebuild_statistics(config, rebuild_db=False, rebuild_statistics=False):
    config.rebuild_db = rebuild_db
    config.rebuild_statistics = rebuild_statistics
    build_lpbound_statistics(config)

def parse_alias_mapping(sql):
    from_match = re.search(r"\bFROM\b(.*?)(\bWHERE\b|$)", sql, flags=re.IGNORECASE | re.DOTALL)
    if not from_match:
        return {}
    from_part = from_match.group(1)
    alias_to_rel = {}
    for item in from_part.split(","):
        token = item.strip()
        m = re.match(r"([A-Za-z_][\w]*)\s+(?:AS\s+)?([A-Za-z_][\w]*)$", token, flags=re.IGNORECASE)
        if m:
            rel = m.group(1).upper()
            alias = m.group(2).upper()
            alias_to_rel[alias] = rel
            alias_to_rel[rel] = rel
        else:
            m2 = re.match(r"([A-Za-z_][\w]*)$", token, flags=re.IGNORECASE)
            if m2:
                rel = m2.group(1).upper()
                alias_to_rel[rel] = rel
    return alias_to_rel

def sync_join_variables_from_drift(schema_path, drift_dir):
    with open(schema_path, "r") as f:
        schema = json.load(f)
    join_vars = schema.get("join_variables", {})
    query_files = glob.glob(os.path.join(drift_dir, "test", "*.txt")) + glob.glob(os.path.join(drift_dir, "train", "*.txt"))
    eq_pattern = re.compile(
        r"([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)",
        flags=re.IGNORECASE,
    )
    for path in query_files:
        with open(path, "r") as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip()
            if "#####" not in line:
                continue
            sql = line.split("#####", 1)[1]
            alias_map = parse_alias_mapping(sql)
            for m in eq_pattern.finditer(sql):
                a1, c1, a2, c2 = m.group(1).upper(), m.group(2).upper(), m.group(3).upper(), m.group(4).upper()
                r1 = alias_map.get(a1)
                r2 = alias_map.get(a2)
                if r1 is not None:
                    join_vars.setdefault(r1, ["ID"])
                    if c1 not in join_vars[r1]:
                        join_vars[r1].append(c1)
                if r2 is not None:
                    join_vars.setdefault(r2, ["ID"])
                    if c2 not in join_vars[r2]:
                        join_vars[r2].append(c2)
    schema["join_variables"] = join_vars
    with open(schema_path, "w") as f:
        json.dump(schema, f, indent=2)

def process_directory(config, dir_path, output_csv):
    results = []
    txt_files = glob.glob(os.path.join(dir_path, "*.txt"))
    total = 0
    max_queries = getattr(config, "_max_queries", 0)
    for file_path in tqdm(txt_files, desc=f"Processing {os.path.basename(dir_path)}"):
        file_name = os.path.basename(file_path)
        with open(file_path, "r") as f:
            lines = f.readlines()
        for line in lines:
            if max_queries > 0 and total >= max_queries:
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split("#####")
            if len(parts) != 2:
                continue
            query_id = parts[0]
            original_sql = parts[1]
            clean_sql = remove_unsupported_predicates(original_sql)
            try:
                est = estimate(clean_sql, config, verbose=False)
                status = "ok"
                error_msg = ""
            except Exception as e:
                print('exception:', e)
                from lpbound.config.benchmark_schema import load_benchmark_schema
                from lpbound.duckdb_adapter.duckdb_manager import DatabaseManager
                from lpbound.acyclic.join_graph.sql_parser import parse_sql_query_to_join_graph

                schema_data: BenchmarkSchema = load_benchmark_schema(config)
                con: DuckDBPyConnection = DatabaseManager(schema_data).create_or_load_db(read_only=True)
                jg = parse_sql_query_to_join_graph(clean_sql, schema_data)
                print(jg)
                con.close()

                est = -1.0
                status = f"error:{type(e).__name__}"
                error_msg = str(e)
            results.append({
                "File": file_name,
                "QueryID": query_id,
                "OriginalSQL": original_sql,
                "CleanSQL": clean_sql,
                "Estimate": est,
                "Status": status,
                "Error": error_msg
            })
            total += 1
        if max_queries > 0 and total >= max_queries:
            break
    df = pd.DataFrame(results)
    df.to_csv(output_csv, index=False)
    print(f"Saved results to {output_csv}")
    print(df["Status"].value_counts())
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--split", choices=["test", "train", "both"], default="both")
    parser.add_argument("--rebuild-db", action="store_true")
    parser.add_argument("--rebuild-statistics", action="store_true")
    parser.add_argument("--max-queries", type=int, default=0)
    parser.add_argument("--sync-join-vars", action="store_true")
    parser.add_argument("--p-max", type=int, default=10)
    parser.add_argument("--num-mcvs", type=int, default=5000)
    parser.add_argument("--num-buckets", type=int, default=128)
    args = parser.parse_args()

    config = LpBoundConfig(
        benchmark_name="imdb",
        p_max=args.p_max,
        num_mcvs=args.num_mcvs,
        num_buckets=args.num_buckets,
        rebuild_db=args.rebuild_db,
        rebuild_statistics=args.rebuild_statistics,
    )
    setattr(config, "_max_queries", args.max_queries)
    if args.sync_join_vars:
        schema_path = str(LpBoundPaths.SCHEMAS_DIR / "imdb_schema.json")
        drift_dir = str(LpBoundPaths.DATASETS_DIR / "IMDB_DATA_DRIFT")
        sync_join_variables_from_drift(schema_path, drift_dir)
    maybe_rebuild_statistics(config, args.rebuild_db, args.rebuild_statistics)

    base_dir = str(LpBoundPaths.DATASETS_DIR / "IMDB_DATA_DRIFT")
    os.makedirs("results", exist_ok=True)
    all_frames = []
    splits = ["test", "train"] if args.split == "both" else [args.split]
    for split in splits:
        split_dir = os.path.join(base_dir, split)
        output_csv = f"results/drift_{split}_results.csv"
        df = process_directory(config, split_dir, output_csv)
        df.insert(0, "Split", split)
        all_frames.append(df)
    if len(all_frames) > 1:
        merged = pd.concat(all_frames, ignore_index=True)
        merged.to_csv("results/drift_all_results.csv", index=False)
        print("Saved results to results/drift_all_results.csv")
        print(merged["Status"].value_counts())

if __name__ == "__main__":
    main()
