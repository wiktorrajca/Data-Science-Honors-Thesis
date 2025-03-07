# **Procurement Fraud Detection Graph**

## **Overview**

This system constructs a **network graph** of procurement winners and their ownership structures using various datasets, including procurement data, company ownership information, and sanctioned entity lists. The graph helps analyze relationships and detect potential fraud.

## **Usage Instructions**

### **1. Match Procurement Data with Orbis Data**

To integrate procurement data with Orbis company data, use matcher.py. This process assigns a **BvD ID** to each bid winner if available in Orbis.

#### **Example Command:**

```bash
python matcher.py --base_file ted_procurement_data.csv --additional_dir orbis_data_folder --output_dir output
```

- Ensure that **TED procurement dataset** has a column named `WIN_COUNTRY_CODE`.
- The output will be a **procurement CSV** with mapped BvD IDs.

### **2. Match Sanctioned Lists with Orbis Data**

To integrate sanctioned entities with Orbis data, use matcher.py.

#### **Example Command:**

```bash
python matcher.py --base_file sanctioned_list.csv --additional_dir orbis_data_folder --output_dir output --name_column1 sanctioned_name_column
```

- Specify the **column containing names of sanctioned entities** using `--name_column1`.
- The output will be a **sanctioned list CSV** with mapped BvD IDs.

### **3. Organizing Ownership Datasets**

The system uses different datasets for company ownership structures:

- **Subsidiaries dataset** (`subsidiaries.csv`)
- **Basic shareholders dataset** (`basic_shareholders.csv`)
- **First-level shareholders dataset** (`first_level_shareholders.csv`)
- **(Not used) Controlling shareholders dataset** – ownership control is inferred from `basic_shareholders.csv`.

### **4. Generate the Graph with main.py **

To construct the graph, run main.py.

#### **Command Format:**

```bash
python main.py --country PL FR --subsidiary_folder path_to_subsidiaries --shareholder_folder path_to_basic_shareholders --first_level_shareholders_folder path_to_first_level_shareholders
```

- ``: Specify one or multiple country codes (e.g., PL DE FR). If omitted, graphs are generated for all countries.
- ``: Path to folder containing subsidiary datasets.
- ``: Path to folder containing basic shareholder datasets.
- ``: Path to folder containing first-level shareholder datasets.

The system will:

1. **Load or initialize a graph** for each country.
2. **Add procurement winners** from procurement datasets.
3. **Expand the ownership structure** using different shareholder datasets.
4. **Add flagged entities** (sanctioned companies/individuals that match an entity in the graph).
5. **Save the graph** for future use.

### **5. Graph Output Format**

- The graphs are stored in **GraphML format** (`.graphml` files).
- Each node in the graph contains metadata from the datasets.
- **Edges represent relationships** such as `WON`, `OWNS`, `CONTROLLS` and `SUBSIDIARY_OF`.

## **Notes & Recommendations**

- **Ensure dataset formatting is correct** before running the scripts.
- **Run **matcher.py** before **`` to assign BvD IDs properly.
- **Graphs grow over time**, so the system is designed to prevent duplicate entries when re-running the scripts.

### **Example Workflow**

```bash
# Step 1: Match Procurement Data
python matcher.py --base_file ted_procurement_data.csv --additional_dir orbis_data_folder --output_dir output

# Step 2: Match Sanctioned Lists
python matcher.py --base_file sanctioned_list.csv --additional_dir orbis_data_folder --output_dir output --name_column1 sanctioned_name_column

# Step 3: Generate Graph for Poland
python main.py --country PL --subsidiary_folder path_to_subsidiaries --shareholder_folder path_to_basic_shareholders --first_level_shareholders_folder path_to_first_level_shareholders
```

The system will now generate a **structured procurement fraud detection graph**!

