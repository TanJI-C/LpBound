from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional
import duckdb
import os

from duckdb import DuckDBPyConnection

from lpbound.config.benchmark_schema import BenchmarkSchema
from lpbound.config.paths import LpBoundPaths




class DatabaseManager:
    META_TABLE_NAME = "__lpbound_internal_meta"
    DB_IMPORT_COMPLETE_KEY = "db_import_complete"
    STATISTICS_COMPLETE_KEY = "statistics_complete"

    def __init__(self, benchmark_schema: BenchmarkSchema, groupby: bool = False):
        """benchmark: str, e.g., "jobjoin", "joblight", "jobrange", "stats" subgraph_matching"""
        self.benchmark: str = benchmark_schema["name"]  # e.g., "jobjoin"
        self.db_name: str = LpBoundPaths.WORKLOAD_TO_DB_MAP[self.benchmark] if not groupby else LpBoundPaths.GROUPBY_DB_NAME_DICT[self.benchmark]
        self.duckdb_file: str = os.path.join(LpBoundPaths.DATA_DIR, "duckdb", f"{self.db_name}_duckdb.db")
        self.benchmark_schema: BenchmarkSchema = benchmark_schema

        self.csv_data_dir: str = LpBoundPaths.CSV_DATA_DIR_MAP[self.db_name]

    def create_or_load_db(self, read_only: bool = False, rebuild_db: bool = False) -> DuckDBPyConnection:
        if os.path.exists(self.duckdb_file) and not read_only and rebuild_db:
            os.remove(self.duckdb_file)
        if os.path.exists(self.duckdb_file):
            con = duckdb.connect(database=self.duckdb_file, read_only=True)
            if self._is_db_import_complete(con):
                if read_only:
                    return con
                con.close()
                return duckdb.connect(database=self.duckdb_file, read_only=False)
            con.close()
            os.remove(self.duckdb_file)

        con = self._create_fresh_db_with_data()
        if read_only:
            con.close()
            return duckdb.connect(database=self.duckdb_file, read_only=True)
        return con

    def has_statistics(self, con: DuckDBPyConnection) -> bool:
        return self._get_meta_value(con, self.STATISTICS_COMPLETE_KEY) == "1"

    def mark_statistics_state(self, con: DuckDBPyConnection, is_complete: bool):
        self._ensure_meta_table(con)
        value = "1" if is_complete else "0"
        self._set_meta_value(con, self.STATISTICS_COMPLETE_KEY, value)

    def _create_fresh_db_with_data(self) -> DuckDBPyConnection:
        os.makedirs(os.path.dirname(self.duckdb_file), exist_ok=True)
        con: DuckDBPyConnection = duckdb.connect(database=self.duckdb_file, read_only=False)
        self._ensure_meta_table(con)
        self._set_meta_value(con, self.DB_IMPORT_COMPLETE_KEY, "0")
        self._set_meta_value(con, self.STATISTICS_COMPLETE_KEY, "0")
        self.create_and_load_db_tables(con)
        self.create_db_indexes(con)
        self._set_meta_value(con, self.DB_IMPORT_COMPLETE_KEY, "1")
        self._set_meta_value(con, self.STATISTICS_COMPLETE_KEY, "0")
        return con

    def _is_db_import_complete(self, con: DuckDBPyConnection) -> bool:
        return self._get_meta_value(con, self.DB_IMPORT_COMPLETE_KEY) == "1"

    def _ensure_meta_table(self, con: DuckDBPyConnection):
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.META_TABLE_NAME} (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT NOT NULL
            );
            """
        )

    def _set_meta_value(self, con: DuckDBPyConnection, key: str, value: str):
        con.execute(
            f"""
            INSERT INTO {self.META_TABLE_NAME}(meta_key, meta_value)
            VALUES (?, ?)
            ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value;
            """,
            [key, value],
        )

    def _get_meta_value(self, con: DuckDBPyConnection, key: str) -> str | None:
        row = con.execute(
            f"SELECT meta_value FROM {self.META_TABLE_NAME} WHERE meta_key = ? LIMIT 1;",
            [key],
        ).fetchone()
        return None if row is None else row[0]

    def create_and_load_db_tables(self, con: DuckDBPyConnection):
        relations = self.benchmark_schema["relations"]
        relation_names = relations.keys()

        # create tables
        file_path = os.path.join(LpBoundPaths.DATA_DIR, "sql_scripts", "duckdb", f"create_queries_{self.db_name}.sql")
        # read the whole file and execute it
        with open(file_path, "r") as f:
            con.sql(f.read())
        print("All tables created.")

        # import data into tables
        for name in relation_names:
            file_name = name.lower()
            if self.benchmark == "subgraph_matching":  #
                if name == "dblp.edge":
                    file_name = "dblp_edge_undirected"
                elif name == "dblp.vertex":
                    file_name = "dblp_vertex"

            csv_path = f"{LpBoundPaths.DATA_DIR}/datasets/{self.csv_data_dir}/{file_name}.csv"
            with open(csv_path, "r") as f:
                first_line = f.readline()
            delimiter = "|" if "|" in first_line else ","
            insert_query = (
                f"COPY {name} FROM '{csv_path}' "
                f"(AUTO_DETECT FALSE, HEADER TRUE, DELIMITER '{delimiter}', NULLSTR '');"
            )
            con.execute(insert_query)
        print("All data inserted.")

    def create_db_indexes(self, con: DuckDBPyConnection):
        file_path = os.path.join(LpBoundPaths.DATA_DIR, "sql_scripts", "duckdb", f"fkindexes_{self.db_name}.sql")
        with open(file_path, "r") as f:
            con.sql(f.read())
        print("All indices created. \n")
