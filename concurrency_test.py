import time
from multiprocessing import Pool
from cpu_stress import cpu_stress

def run_local_concurrency_test(num_jobs, intensity):
    print(f"Starting {num_jobs} concurrent jobs locally...")
    start_all = time.time()
    
    with Pool(processes=num_jobs) as pool:
        pool.map(cpu_stress, [intensity] * num_jobs)
        
    end_all = time.time() 
    
    print(f"\n--- ALL JOBS FINISHED ---")
    print(f"Total Wall-Clock Time: from {start_all} to {end_all}")

if __name__ == "__main__":
    run_local_concurrency_test(num_jobs=32, intensity=20_000_000)