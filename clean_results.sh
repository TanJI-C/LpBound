#!/bin/bash

# Clean generated result files while keeping static ground-truth data (e.g., true cardinalities).
# A backup of the current results directory is saved under results_backup/.

set -euo pipefail

timestamp=$(date +"%Y%m%d_%H%M%S")
backup_dir="results_backup/${timestamp}"

mkdir -p "${backup_dir}"
cp -r results "${backup_dir}"

# remove from results/accuracy_acyclic
# remove generated outputs while keeping true cardinalities
rm -f results/accuracy_acyclic/*/lpbound_*
rm -f results/accuracy_acyclic/*/postgres_*
rm -f results/accuracy_acyclic/*/duckdb_*

# remove from results/accuracy_groupby
rm -f results/accuracy_groupby/*/lpbound_*
rm -f results/accuracy_groupby/*/postgres_*
rm -f results/accuracy_groupby/*/duckdb_*

# remove from results/evaluation_time
rm -f results/evaluation_time/*/lpbound_*
rm -f results/evaluation_time/*/postgres_*
rm -f results/evaluation_time/*/duckdb_*

# remove from results/evaluation_time/subquery_estimations
rm -f results/evaluation_time/subquery_estimations/*/lpbound_*
rm -f results/evaluation_time/subquery_estimations/*/postgres_*
rm -f results/evaluation_time/subquery_estimations/*/duckdb_*

# remove generated files from results/estimation_time, results/statistics_computation_time, results/space_usage
rm -f results/estimation_time/*
rm -f results/statistics_computation_time/*
rm -f results/space_usage/*

# remove everything from results/mcv_effectiveness
rm -rf results/mcv_effectiveness
mkdir -p results/mcv_effectiveness
mkdir -p results/mcv_effectiveness/joblight
mkdir -p results/mcv_effectiveness/jobrange

# remove everything from results/num_norms_effectiveness
rm -rf results/num_norms_effectiveness
mkdir -p results/num_norms_effectiveness
mkdir -p results/num_norms_effectiveness/joblight

echo "Results cleaned successfully. Backup created at ${backup_dir}."
