# Data Log Analysis and Transaction Report

## 1. Project Summary
This project analyzes raw event logs for **customer accounts**, **cards**, and **savings accounts** to produce a unified, denormalized view and a detailed transaction report.  

The script is designed to run in two environments:
- **Local Environment**: Directly using Python for development and testing.  
- **Docker Environment**: Packaged in a container for consistent, isolated execution.  

The script performs three main tasks:
1. **Task 1**: Prints out each state of the `accounts`, `cards`, and `savings_accounts` tables from event logs.  
2. **Task 2**: Merges these tables into a single, denormalized view.  
3. **Task 3**: Analyzes the logs to identify and report all financial transactions.  

---

## 2. Requirements

### Local Development
- Python 3.7+  
- Pandas & NumPy libraries  

Install dependencies:
```bash
pip install -r requirements.txt
```

## Docker Deployment
Docker Desktop installed and running

## 3. File Structure
```
project/
├── data/
│   ├── accounts/
│   ├── cards/
│   └── savings_accounts/
├── solution/
│   └── solution.py      # The main Python script
├── Dockerfile           # Instructions to build the Docker image
├── requirements.txt     # Python dependencies
└── run_docker.sh        # Automation script for Docker
```

## 4. How to Run
# Method A: Running Locally
1. Install Dependencies
``` bash
pip install -r requirements.txt
```

2. Run the Script
``` bash
python solution/solution.py
```

3.  View Output.


# Method B: Running with Docker (Recommended)
1. Open Terminal and navigate using cd to the root project/ directory.
2. Make the script executable.
``` bash
chmod +x run_docker.sh
```

3. Build and Run
``` bash
./run_docker.sh
```

4. View Output in Terminal.
