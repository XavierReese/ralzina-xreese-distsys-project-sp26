import time

def cpu_stress(n):
    """A CPU-bound task that calculates sum of squares."""
    start_time = time.time()
    
    _ = sum(i*i for i in range(n))
    
    end_time = time.time()
    duration = end_time - start_time
    print(f"Job finished in {duration:.4f} seconds.")
    return duration

if __name__ == "__main__":
    cpu_stress(20_000_000)