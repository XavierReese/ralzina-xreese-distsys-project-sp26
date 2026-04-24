import os
import re

def get_max_runtime():
    # Get the directory where this script is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Regex pattern: 'client' followed by 1 or more digits, ending in '.log'
    file_pattern = re.compile(r'^client\d+\.log$')

    line_pattern = re.compile(r'^(?:> )?(\d+\.\d+)$')

    start_time = float('inf')
    end_time = 0

    individual_time = []
    total_times = 0

    # Walk through the current directory
    for filename in os.listdir(current_dir):
        individual_time = []
        if file_pattern.match(filename):
            file_path = os.path.join(current_dir, filename)
            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        line_s = line.strip()

                        match = line_pattern.match(line_s)
                        if match:
                            individual_time.append(float(match.group(1)))

            except Exception as e:
                print(f"[ERROR] Could not read {filename}: {e}")
            
            if len(individual_time) == 2:
                start_time = min(start_time, individual_time[0])
                end_time = max(end_time, individual_time[1])
            total_times += 1

    print(f"{total_times} {end_time-start_time}")

if __name__ == "__main__":
    get_max_runtime()
