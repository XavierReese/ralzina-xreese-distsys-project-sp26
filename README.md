# ralzina-xreese-distsys-project-sp26

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
The second n is how many workers where running when you called n_clients.sh

Step 5:<br>
Repeat with any combination of n's you want