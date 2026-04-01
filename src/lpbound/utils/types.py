from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional
AliasColPair = Tuple[str, str]  # (alias, column)
Stats = Dict[AliasColPair, List[float]]  # [(alias, join_column)] -> [lp_norm]
DomainSizeStats = Dict[str, int]  # [alias] -> domain_size
