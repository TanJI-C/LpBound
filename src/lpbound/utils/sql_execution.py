from __future__ import annotations
import time
import threading
import re
import os
import multiprocessing
from typing import Any, Dict, List
import duckdb
from tqdm import tqdm
from collections import defaultdict

from duckdb import DuckDBPyConnection

from lpbound.acyclic.stat_generator.sql_utils import SqlCommand


def _inject_fkpk_range_sampling(sql_text: str, ratio: float) -> str:
    if ratio >= 1.0:
        return sql_text
    ratio = max(0.001, min(1.0, ratio))
    scale = 1.0 / ratio
    sampled = re.sub(r"SUM\(deg\)\s+as\s+deg", f"SUM(deg) * {scale} as deg", sql_text, count=1, flags=re.IGNORECASE)
    sampled = re.sub(
        r"WHERE\s+([A-Za-z_][\w]*)\s+IS\s+NOT\s+NULL\s+AND\s+bucket_id\s+IS\s+NOT\s+NULL",
        r"WHERE \1 IS NOT NULL AND bucket_id IS NOT NULL AND random() < " + f"{ratio}",
        sampled,
        count=1,
        flags=re.IGNORECASE,
    )
    return sampled


def _get_duckdb_file_path(con: DuckDBPyConnection) -> str | None:
    # 暂时保留该函数作为兼容方案
    db_entries = con.execute("PRAGMA database_list").fetchall()
    for _, _, path in db_entries:
        if path and path != ":memory:":
            return path
    return None


def _worker_execute_sql(db_path: str, sql_variant: str, return_dict: dict):
    """
    Subprocess worker that executes the SQL variant.
    """
    try:
        # Re-connect in the child process
        con = duckdb.connect(database=db_path, read_only=False)
        con.execute(sql_variant)
        con.close()
        return_dict["ok"] = True
    except Exception as e:
        return_dict["error"] = e


def _execute_fkpk_range_with_timeout(
    con: DuckDBPyConnection, sql_text: str, timeout_seconds: int = 180, duckdb_file: str | None = None, past_commands: List[str] = None
) -> tuple[float, DuckDBPyConnection]:
    sample_ratios = [1.0, 0.5, 0.25, 0.1, 0.05]
    last_error: Exception | None = None
    try_num = 0
    current_con = con
    past_commands = past_commands or []

    for ratio in sample_ratios:
        sql_variant = _inject_fkpk_range_sampling(sql_text, ratio)
        finished = threading.Event()
        result: Dict[str, Any] = {}

        def run_query():
            try:
                current_con.execute(sql_variant)
                result["ok"] = True
            except Exception as e:
                result["error"] = e
            finally:
                finished.set()
        if try_num != 0:
            print(f"run query with ratio={ratio}, try_num={try_num}")
        worker = threading.Thread(target=run_query, daemon=True)
        worker.start()

        if finished.wait(timeout_seconds):
            if "error" in result:
                last_error = result["error"]
                raise last_error
            return ratio, current_con

        print(f"[INFO] FKPK_RANGE timeout {timeout_seconds} seconds, interrupted with ratio={ratio}")
        
        # We need to interrupt the connection to kill the hanging thread.
        for _ in range(10):
            try:
                current_con.interrupt()
            except Exception:
                pass
            if finished.wait(0.5):
                break

        if finished.wait(2):
            if "error" in result:
                # If error is just an interrupt error, that's expected
                if "Interrupt" in str(result["error"]) or "interrupted" in str(result["error"]).lower():
                    try_num += 1
                    continue
                last_error = result["error"]
                try_num += 1
                continue
            return ratio, current_con
        else:
            print(f"[WARN] DuckDB thread stuck at ratio={ratio}. Rebuilding connection and replaying history...")
            try:
                current_con.close()
            except Exception:
                pass
            
            db_path = duckdb_file if duckdb_file else _get_duckdb_file_path(current_con)
            if not db_path:
                raise TimeoutError("Cannot reconnect because duckdb_file is unknown.")

            try:
                current_con = duckdb.connect(database=db_path, read_only=False)
            except Exception as e:
                raise RuntimeError(f"Failed to reconnect after stuck thread: {e}")
            
            # Replay all previous commands to restore temp tables and session state
            for prev_sql in past_commands:
                try:
                    current_con.execute(prev_sql)
                except Exception as e:
                    # Some tables might already exist if they are physical, just ignore errors
                    if "already exists" not in str(e).lower():
                        raise RuntimeError(f"Failed to replay history query: {e}\nQuery: {prev_sql}")

            try_num += 1

    if last_error is not None:
        raise last_error
    raise RuntimeError("FKPK_RANGE execution failed without exception")


def execute_with_timing(commands: List[SqlCommand], con: DuckDBPyConnection, duckdb_file: str | None = None) -> Dict[str, float]:
    """
    Execute each command in 'commands', measure how long each takes.
    Returns dict { tag -> total_time_in_seconds }.
    """
    times_per_tag: defaultdict[str, float] = defaultdict(float)
    past_executed_sqls: List[str] = []

    with tqdm(total=len(commands), desc="Executing queries") as pbar:
        for cmd in commands:
            sql_text: str = cmd["sql"]
            tag: str = cmd["tag"]
            # print(f"Executing query: {tag}")
            # print(sql_text)
            # print("-" * 100)

            pbar.set_postfix_str(f"Current: {tag}")

            start = time.perf_counter()
            if tag == "FKPK_RANGE":
                timeout_seconds = int(os.getenv("LPB_FKPK_TIMEOUT_SECONDS", "180"))
                used_ratio, con = _execute_fkpk_range_with_timeout(
                    con, sql_text, timeout_seconds=timeout_seconds, duckdb_file=duckdb_file, past_commands=past_executed_sqls
                )
                if used_ratio < 1.0:
                    print(f"[INFO] FKPK_RANGE timeout, retried with sampling ratio={used_ratio}")
                    print(f"Executing query: {tag}")
                    print(sql_text)
                    print("-" * 100)
                
                # Append the successfully executed variant to history
                past_executed_sqls.append(_inject_fkpk_range_sampling(sql_text, used_ratio))
            else:
                con.execute(sql_text)
                past_executed_sqls.append(sql_text)

            end = time.perf_counter()

            times_per_tag[tag] += end - start
            pbar.update(1)

    return times_per_tag


def write_commands_to_file(commands: List[SqlCommand], output_file: str) -> None:
    """
    Write the commands to a file as raw SQL (plus comments).
    """
    with open(output_file, "w") as f_out:
        for cmd in commands:
            f_out.write(cmd["sql"] + "\n\n")
    print(f"[INFO] Wrote {len(commands)} commands to {output_file}")


def execute_fetchone_sql(con: DuckDBPyConnection, sql: str) -> List[Any]:
    """Execute SQL and return the result."""
    return con.execute(sql).fetchone()


def get_time_breakdown_string(times_dict: Dict[str, float]) -> str:
    """Get a string with the time breakdown."""
    s = "=== Execution Time Breakdown by Tag ===\n"
    overall_time = sum(times_dict.values())
    # sort by the values
    items = [item for item in sorted(times_dict.items(), key=lambda x: x[1], reverse=True)]
    for tag, total_time in items:
        s += f"{tag}: {total_time:.3f} seconds -- {total_time * 100 / overall_time:.3f}%\n"
    s += f"Total time: {overall_time:.3f} seconds\n"
    return s


def get_overall_time(times_dict: Dict[str, float]) -> float:
    """Get the overall time."""
    return sum(times_dict.values())
