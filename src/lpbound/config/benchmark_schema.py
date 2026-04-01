from __future__ import annotations
import json
from typing import TypedDict

from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.config.paths import LpBoundPaths


# Define types for the foreign key to primary key join structure
class FkPkJoin(TypedDict):
    pk_relation: str
    fk: str
    pk: str


# Define the main schema type with more generic typing
class BenchmarkSchema(TypedDict):
    name: str
    relations: Dict[str, Dict[str, str]]  # Relation name -> (Column name -> Type)
    join_variables: Dict[str, List[str]]  # Relation name -> List of join columns
    groupby_variables: Dict[str, List[List[str]]]  # Relation name -> List of groupby column combinations
    equality_predicate_variables: Dict[str, List[str]]  # Relation name -> List of equality predicate columns
    range_predicate_variables: Dict[str, List[str]]  # Relation name -> List of range predicate columns
    fk_pk_joins_dict: Dict[str, List[FkPkJoin]]  # FK relation -> List of FK-PK join specifications


def load_benchmark_schema(lpbound_config: LpBoundConfig) -> BenchmarkSchema:
    """
    Loads the JSON file that defines the schema (relations, join_variables, etc.).
    Works with any schema following the expected structure.
    """
    from pathlib import Path
    
    schema_path = Path(f"{LpBoundPaths.SCHEMAS_DIR}/{lpbound_config.benchmark_name}_schema.json")
    
    # Load the JSON specific schema if exists
    if schema_path.exists():
        with open(schema_path, "r") as f:
            data: BenchmarkSchema = json.load(f)
        return data
    else:
        raise FileNotFoundError(f"Schema file not found at {schema_path}")
