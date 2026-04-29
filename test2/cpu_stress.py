import time

def cpu_stress(n):
    """A CPU-bound task that calculates sum of squares."""    
    _ = sum(i*i for i in range(n))
    
if __name__ == "__main__":
    cpu_stress(500_000_000)