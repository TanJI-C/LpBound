from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional
import duckdb
from filelock import FileLock
import os
#!/usr/bin/env python3
"""
execute_sqls.py

1) Builds/loads a DuckDB database connection via DatabaseManager.
2) Generates the Lp-norm SQL commands from sql_generation.py for the chosen benchmark.
3) Executes them one by one, measuring time per 'tag'.
4) Prints a time breakdown.

Usage:
  python execute_sqls.py
"""

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.acyclic.stat_generator.sql_statistics_generator import generate_all_sql_for_benchmark

# from lpbound.acyclic.stat_generator.sm_experiments import generate_sm_commands
from lpbound.config.paths import LpBoundPaths
from lpbound.config.benchmark_schema import load_benchmark_schema
from lpbound.acyclic.stat_generator.sql_utils import SqlCommand
from lpbound.duckdb_adapter.duckdb_manager import DatabaseManager

from lpbound.utils.sql_execution import execute_with_timing, get_time_breakdown_string, write_commands_to_file


def create_lpbound_statistics(lpbound_config: LpBoundConfig) -> Dict[str, float]:
    print(f"create_lpbound_statistics: {lpbound_config.benchmark_name}")
    schema_data = load_benchmark_schema(lpbound_config)

    # 2) Build or load the DB connection
    #    The DatabaseManager references config.DATA_DIR, etc.
    db_manager = DatabaseManager(benchmark_schema=schema_data)
    
    # 非lock检查（防止多进程冲突）
    if os.path.exists(db_manager.duckdb_file):
        con = duckdb.connect(database=db_manager.duckdb_file, read_only=True)
        if db_manager.has_statistics(con) and db_manager._is_db_import_complete(con):
            print(f"Skip statistics generation because existing statistics are complete: {db_manager.duckdb_file}")
            con.close()
            return {"SKIP_EXISTING_STATISTICS": 0.0}
    
    # Use filelock to prevent multiprocess write conflict
    lock_path = f"{db_manager.duckdb_file}.lock"
    with FileLock(lock_path):
        con = db_manager.create_or_load_db(read_only=False, rebuild_db=lpbound_config.rebuild_db)
        print(f"create_or_load_db: {db_manager.duckdb_file}, read_only=False, rebuild_db={lpbound_config.rebuild_db}")

        db_manager.mark_statistics_state(con, is_complete=False)
    
        # 3) Generate the Lp-norm SQL commands
        commands: List[SqlCommand] = generate_all_sql_for_benchmark(con, lpbound_config)
    
        # 3a) write the commands to a file for inspection
        groupby_suffix = "_groupby" if lpbound_config.enable_groupby else ""
        sql_file = LpBoundPaths.GENERATED_SQL_DIR / f"{lpbound_config.benchmark_name}{groupby_suffix}_lpnorm_queries.sql"
        write_commands_to_file(commands, str(sql_file))
        print(f"Done. See {LpBoundPaths.GENERATED_SQL_DIR} for all generated SQL statements.")
    
        # 4) Execute with timing
        try:
            times_dict = execute_with_timing(commands, con, duckdb_file=db_manager.duckdb_file)
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            state_con = duckdb.connect(database=db_manager.duckdb_file, read_only=False)
            try:
                db_manager.mark_statistics_state(state_con, is_complete=False)
            finally:
                state_con.close()
            raise
    
        # 5) Print a breakdown
        print(get_time_breakdown_string(times_dict))
        try:
            con.close()
        except Exception:
            pass
        state_con = duckdb.connect(database=db_manager.duckdb_file, read_only=False)
        try:
            db_manager.mark_statistics_state(state_con, is_complete=True)
        finally:
            state_con.close()

    return times_dict
