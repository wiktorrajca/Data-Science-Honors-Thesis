# Data-Science-Honors-Thesis
# CSV Merge with Fuzzy & Exact Name Matching

This project contains a Python script that merges a base CSV file with one or more additional CSV files by comparing company names using both exact and fuzzy matching techniques. The script handles preprocessing (e.g., cleaning, transliterating non-Latin characters, and extracting numbers) to improve the matching accuracy. It is designed to efficiently process large datasets by parallelizing the fuzzy matching tasks across multiple CPU cores.

## Features

- **Preprocessing:**  
  - Converts names to lowercase.
  - Removes common company suffixes (e.g., `inc`, `llc`, `corp`).
  - Strips punctuation and normalizes whitespace.
  - Extracts numbers from company names.
  - Transliterates non-Latin characters using `unidecode`.

- **Matching:**  
  - **Exact Matching:** Merges records where preprocessed names and extracted numbers match exactly.
  - **Fuzzy Matching:** Uses RapidFuzz to compute similarity scores for names that require transliteration, with an optimized letter (prefix) check and parallel processing.

- **Parallel Processing:**  
  Utilizes the `multiprocessing` library to speed up the fuzzy matching process by splitting the workload across available CPU cores.

- **Command-Line Interface:**  
  Easily specify input files, directories, and parameters via command-line arguments.

## Dependencies

Ensure you have the following Python packages installed:

- Python 3.6+
- [pandas](https://pandas.pydata.org/)
- [numpy](https://numpy.org/)
- [regex](https://pypi.org/project/regex/)
- [rapidfuzz](https://pypi.org/project/rapidfuzz/)
- [tqdm](https://pypi.org/project/tqdm/)
- [Unidecode](https://pypi.org/project/Unidecode/)

You can install the required packages using pip:

```bash
pip install pandas numpy regex rapidfuzz tqdm Unidecode
