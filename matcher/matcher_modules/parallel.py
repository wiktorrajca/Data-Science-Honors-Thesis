import numpy as np
from multiprocessing import Pool, cpu_count
import pandas as pd

def parallel_match(table1, table2, match_function, similarity_threshold=0.8):
    # Modified version of merge_with_parallel_processing
    num_cores = cpu_count()
    chunks = np.array_split(table1, num_cores)
    
    args = [(chunk, table2, similarity_threshold) for chunk in chunks]
    
    with Pool(processes=num_cores) as pool:
        results = pool.starmap(match_function, args)
        
    return pd.concat(results, ignore_index=True)