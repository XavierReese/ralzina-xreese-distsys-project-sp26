import time

def cpu_stress(n):
    """A CPU-bound task that calculates sum of squares."""
    start_time = time.time()
    
    _ = sum(i*i for i in range(n))
    
    end_time = time.time()

    print(f"Job ran from {start_time} to {end_time}")

if __name__ == "__main__":
    cpu_stress(10_000_000)