# Procurement Corruption Detection - README

## Overview
This project aims to detect procurement fraud by integrating public procurement data (TED), corporate ownership structures (Orbis), and sanction lists into a **graph-based detection system**. The system:

- **Matches procurement award data (TED) with corporate records (Orbis)** to link procurement winners to their corporate structures.
- **Matches sanctioned entities with Orbis** to flag high-risk companies.
- **Builds a graph representation** of procurement fraud risk, incorporating first-level shareholders, subsidiaries, and controlling shareholders.
- **Outputs a structured graph** for analysis of fraud patterns and collusion risks.

This repository contains:
- **`master-script.py`** (Main driver script that processes and organizes the data.)
- **`matcher.py`** (Matches procurement/sanction data with Orbis firm records.)
- **`main.py`** (Builds the fraud detection graph.)

---
## Installation & Dependencies

### **1. Clone the Repository**
```sh
$ git clone https://github.com/wiktorrajca/Data-Science-Honors-Thesis.git
```

### **2. Install Required Dependencies**
Ensure you have **Python 3.7+** installed.
## Dependencies

Ensure you have the following Python packages installed:

- Python 3.6+
- [pandas](https://pandas.pydata.org/)
- [numpy](https://numpy.org/)
- [regex](https://pypi.org/project/regex/)
- [rapidfuzz](https://pypi.org/project/rapidfuzz/)
- [tqdm](https://pypi.org/project/tqdm/)
- [Unidecode](https://pypi.org/project/Unidecode/)
- [networkx](https://networkx.org/)
- [pickle](https://docs.python.org/3/library/pickle.html) (built-in)
- [hashlib](https://docs.python.org/3/library/hashlib.html) (built-in)
- [argparse](https://docs.python.org/3/library/argparse.html) (built-in)
- [os](https://docs.python.org/3/library/os.html) (built-in)
- [glob](https://docs.python.org/3/library/glob.html) (built-in)
- [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) (built-in)
- [shutil] (https://docs.python.org/3/library/shutil.html) (built-in)

You can install the required packages using pip:

```bash
pip install pandas numpy regex rapidfuzz tqdm Unidecode networkx
```

---
## Data Sources
This pipeline relies on:
- **TED Procurement Data** (`export_CAN_2019.csv`): Contains contract awards and winning firms.
- **Sanctions Data** (`open_sanctions.csv`): List of sanctioned entities.
- **Orbis Data**: Corporate records, including ownership structures.
  - `BvD_ID_and_Name files`: Firm identifier dataset.
  - `shareholders_first_level files`: First-level shareholders.
  - `subsidiaries_first_level1 files`: Subsidiaries.
  - `basic_shareholder_info1 files`: Controlling shareholders.
  We assume that all Orbis files are inside the same folder

---
## Running the Pipeline

### **1. Run the Master Script**
This script will:
- Match procurement data with Orbis records.
- Match sanction data with Orbis records.
- Organize Orbis files into structured folders.
- Execute `main.py` to build the fraud detection graph.

### **2. For Professor Deryugina:**
Given the directory structure we have disccused and assuming open_sanctions.csv is in the same directory as TED data please run:
```sh
python3 master-script.py
```

#### **Command Example:**
```sh
python3 master-script.py \
    --ted_data data/TED/export_CAN_2019.csv \
    --sanction_data data/Sanctions/open_sanctions.csv \
    --orbis_dir data/Orbis \
    --output_dir output
```

### **2. Expected Output Directory Structure**
Once execution is complete, `output/` will contain the processed datasets:
```
output/
│── TED/  # Matched procurement data
│── Sanctions/  # Matched sanction data
│── shareholders/  # Organized shareholder files
│── subsidiaries/  # Organized subsidiary files
│── controlling/  # Organized controlling shareholder files
graph_output/  # Final graph files
```

---
## Explanation of Scripts

### **1. `master-script.py`**
- **Finds TED, sanction, and Orbis data**
- **Runs `matcher.py`** to match TED and sanction data with Orbis
- **Organizes Orbis files** into separate directories
- **Runs `main.py`** with all processed files

### **2. `matcher.py`**
- **Matches procurement/sanctions with Orbis** using company names and fuzzy matching.
- **Uses `rapidfuzz` for approximate matching.**

### **3. `main.py`**
- **Constructs a corruption detection graph.**
- **Uses NetworkX** to model firms, ownership links, and flagged entities.
- **Outputs the corruption risk network for analysis.**

---
## Debugging & Logs
- If an error occurs, check which step failed:
  - **Matcher failing?** Check if TED and Orbis files exist in the expected format.
  - **Graph construction failing?** Ensure all matched datasets exist before running `main.py`.
- **Enable debug logs:** Modify `print` statements inside scripts to print more details.

---
## Future Improvements
- **Expand to more datasets** (e.g., additional corporate registries, more procurement datasets).
- **Optimize graph creation per country**
- **Optimize graph performance** (reduce memory usage for large datasets).

---
## Author
**Wiktor Rajca** - UC Berkeley - Data Science Honors Thesis

For questions or contributions, feel free to open an issue or contact via email.

