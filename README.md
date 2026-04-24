# ralzina-xreese-distsys-project-sp26

## Run code
Step 1:<br>
```python Coordinator.py```<br>

Step 2:<br>
```bash n_clients.sh n```<br>
n refers to how many cilents you want

Step 3:<br>
```bash n_workers.sh n```<br>
n refers to how many workers you want

Step 4:<br>
```bash cleanup_n_clients_n_workers n n```<br>
The first n is how many clients where running when you called n_clients.sh<br>
The second n is how many workers where running when you called n_workers.sh

Step 5:<br>
Repeat with any combination of n's you want

## Testing and Plotting
After running the code above and before cleaning up, do the following.

Step 1:<br>
```python time_of_job_per_clients.py >> dataset.txt```<br>
This will count how many clients ran, and it will compute the runtime of every single client from the time the first client their job to the time when the last client received their job. So, this is the total running time of the n_clients.<br><br>
A regular run will print:<br>
<n_clients> <total_runtime_of_all_clients><br><br>
It will then append this to the dataset.txt

Step 2:<br>
```python plot.py```<br>
Creates a logarithmic plot of the time that it took to handle n clients at each of the tests you appended to the dataset.