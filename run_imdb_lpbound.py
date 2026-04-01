import json
import os
import sys
import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'src'))
from lpbound.config.lpbound_config import LpBoundConfig
from lpbound.acyclic.lpbound import build_lpbound_statistics, estimate

def main():
    benchmark = "imdb"
    
    # 1. Build statistics
    print("Building statistics for IMDB...")
    config = LpBoundConfig(
        benchmark_name=benchmark,
        num_mcvs=5000,
        num_buckets=128,
        p_max=10,
    )
    build_lpbound_statistics(config)
    
    # 2. Run estimation
    print("Running estimations...")
    dump_lp_program_path = f"output/lp_programs/{benchmark}/"
    os.makedirs(dump_lp_program_path, exist_ok=True)
    
    with open("data/datasets/imdb/imdb.jsonl", "r") as f:
        lines = f.readlines()
        
    estimates = []
    true_cards = []
    query_ids = []
    
    # Run for first 50 queries for a quick test
    for i, line in tqdm(enumerate(lines), total=len(lines)):
        data = json.loads(line)
        # Use sql_raw because it has the raw table names (e.g. movie_companies instead of imdb_movie_companies)
        # that match the schema definitions.
        query_sql = data["sql_raw"]
        true_card = data.get("true_card", 0)
        
        try:
            this_estimate = estimate(
                input_query_sql=query_sql,
                config=config,
                dump_lp_program_file=f"{dump_lp_program_path}/{benchmark}_estimations_{i}.lp",
                verbose=False,
            )
        except Exception as e:
            print(f"Error on query {i}: {e}")
            this_estimate = -1
            
        estimates.append(this_estimate)
        true_cards.append(true_card)
        query_ids.append(i)
        
    result = pd.DataFrame({
        "QueryID": query_ids,
        "TrueCard": true_cards,
        "LpBound_Estimate": estimates,
    })
    
    # calculate q-error
    def q_error(true, est):
        if est == -1: return None
        true = max(true, 1)
        est = max(est, 1)
        return max(true/est, est/true)
        
    result["Q_Error"] = result.apply(lambda row: q_error(row["TrueCard"], row["LpBound_Estimate"]), axis=1)
    
    print("\nResults Summary:")
    print(result["Q_Error"].describe(percentiles=[0.5, 0.9, 0.95, 0.99]))
    
    result.to_csv(f"results/imdb_lpbound_results.csv", index=False)
    print("Results saved to results/imdb_lpbound_results.csv")

if __name__ == "__main__":
    main()
