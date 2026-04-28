import time
from multiprocessing import Pool
from cpu_stress import cpu_stress

def run_local_concurrency_test(num_jobs, intensity):
    print(f"Starting {num_jobs} concurrent jobs locally...")
    start_all = time.time()
    
    with Pool(processes=num_jobs) as pool:
        pool.map(cpu_stress, [intensity] * num_jobs)
        
    total_duration = time.time() - start_all
    print(f"\n--- ALL JOBS FINISHED ---")
    print(f"Total Wall-Clock Time: {total_duration:.2f} seconds")

if __name__ == "__main__":
    run_local_concurrency_test(num_jobs=32, intensity=20_000_000)