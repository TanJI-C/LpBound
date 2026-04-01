from __future__ import annotations
from typing import Dict, List, Tuple, Any, Optional
from ortools.linear_solver import pywraplp


def entropy(variables: List[str]) -> str:
    sorted_variables = sorted(variables)
    return "_".join((sorted_variables))


def sorted_set(variables: List[str]) -> List[str]:
    return sorted(set(variables))


def print_objective(objective: pywraplp.Objective, variables: Dict[str, pywraplp.Variable]):
    # Build a readable representation of the objective
    terms: List[str] = []
    for var in variables.values():
        coeff = objective.GetCoefficient(var)
        if coeff != 0:  # Only include non-zero coefficients
            term = f"{coeff} * {var.name()}"
            terms.append(term)
    obj_str: str = " + ".join(terms)
    direction: str = "Maximize"
    print(f"{direction}: {obj_str}")
