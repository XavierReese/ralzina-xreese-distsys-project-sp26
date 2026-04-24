import matplotlib.pyplot as plt
import os

def plot_scalability_data():
    x_values = []
    y_values = []
    
    # Get current directory path
    file_path = os.path.join(os.path.dirname(__file__), 'dataset.txt')
    
    # 1. Read the data
    try:
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    parts = line.split()
                    x_values.append(int(parts[0]))
                    y_values.append(float(parts[1]))
    except FileNotFoundError:
        print("Error: dataset.txt not found in the script directory.")
        return

    # 2. Create the plot
    plt.figure(figsize=(10, 6))
    
    # Plotting lines and points
    plt.plot(x_values, y_values, marker='o', linestyle='-', color='#1f77b4', linewidth=2, markersize=8, label='Execution Time')

    # 3. Apply Log Scale to X
    plt.xscale('log', base=2) 
    
    # 4. Force X-axis labels to match your specific datapoints
    plt.xticks(x_values, labels=[str(x) for x in x_values])

    # 5. Add Labels and Styling
    plt.title('System Scalability: Execution Time vs. Client Count', fontsize=14, fontweight='bold')
    plt.xlabel('Number of Clients (Log Scale)', fontsize=12)
    plt.ylabel('Time (Seconds)', fontsize=12)
    plt.grid(True, which="both", ls="-", alpha=0.5)

    plt.tight_layout()
    
    # Show the result
    plt.savefig('plot.png', dpi=300, bbox_inches='tight')

if __name__ == "__main__":
    plot_scalability_data()