# Sanctions & Debarment Datasets

This repository contains a collection of datasets related to sanctions and debarment. The datasets provide information on individuals and companies that have been sanctioned or deemed ineligible for participation in certain activities due to engagement in prohibited practices such as fraud, corruption, collusion, coercion, or obstruction.

These datasets originate from various international regulatory bodies and financial institutions. Some files are directly downloadable (in various formats), while others require additional preprocessing or web scraping.

## Datasets Included

### 1. Denied Persons List
- **Description:**  
  A blacklist of individuals and companies banned by the Bureau of Industry and Security (BIS) for violating U.S. export laws. American companies are prohibited from engaging with any entity on this list.
- **Link:**  
  [Denied Person List](https://www.bis.doc.gov/index.php/policy-guidance/lists-of-parties-of-concern/denied-persons-list) 
- **File Format:**  
  `.txt` (requires extra preprocessing to extract structured data)
- **Notes:**  
  The raw text file needs to be parsed and cleaned before it can be used in your analysis or merging operations.

### 2. Inter-American Development Bank – Sanctioned Firms and Individuals
- **Description:**  
  Contains records of firms and individuals sanctioned for engaging in fraudulent, corrupt, collusive, coercive, or obstructive practices (collectively, “Prohibited Practices”) in violation of the IDB Group’s Sanctions Procedures and anti-corruption policies.
- **Link:**  
  [Inter-American Development Bank Sanctions Data](https://www.iadb.org/en/who-we-are/transparency/sanctions-system/sanctioned-firms-and-individuals)  
  *(Replace `#` with the actual URL if available.)*

### 3. World Bank Listing of Ineligible Firms and Individuals
- **Description:**  
  Lists entities (firms and individuals) that have engaged in prohibited practices such as fraud, corruption, collusion, coercion, or obstruction in World Bank-financed projects.
- **Link:**  
  [World Bank Ineligible Entities](https://www.worldbank.org/en/projects-operations/procurement/debarred-firms)

### 4. Australian Autonomous Financial Sanctions
- **Description:**  
  Includes individuals or entities that have violated Australia’s foreign policy interests or international obligations.
- **Link:**  
  [Australian Autonomous Financial Sanctions](https://www.dfat.gov.au/international-relations/security/sanctions/consolidated-list)

### 5. European Bank for Reconstruction and Development (EBRD) Ineligible Firms
- **Description:**  
  Records of entities and individuals that are ineligible to become a Bank Counterparty. Reasons include engaging in any prohibited practices (as defined in Section II(46) of the Enforcement Policy and Procedures), being subject to a Third Party Finding, or being subject to a Debarment Decision by a Mutual Enforcement Institution.
- **Link:**  
  [EBRD Ineligible Firms](https://ebrd.com/ineligible-entities.html)

### 6. African Development Bank (AfDB) Group List of Debarred Entities
- **Description:**  
  Contains entities that have been sanctioned for participating in coercive, collusive, corrupt, fraudulent, or obstructive practices under the AfDB sanctions system or under the Agreement for Mutual Enforcement of Debarment Decisions.
- **Link:**  
  [AfDB Debarred Entities](https://www.afdb.org/en/projects-operations/debarment-and-sanctions-procedures)

### 7. Financial Consumer Alert by Bank Negara Malaysia
- **Description:**  
  Lists individuals and companies that have engaged in activities violating Malaysian financial laws and regulations.
- **Link:**  
  [Bank Negara Malaysia Financial Consumer Alert](https://www.bnm.gov.my/financial-consumer-alert-list)

### 8. Open Sanctions Debarred Companies and Individuals
- **Description:**  
  An open-source dataset aggregating lists of sanctioned entities from various sources. It includes nearly 206,000 records covering individuals, legal entities, and companies from over 190 countries. Many of the datasets mentioned above are also incorporated into this comprehensive dataset.
- **Link:**  
  [Open Sanctions Data](https://www.opensanctions.org/datasets/debarment/)

## License

This project is licensed under the [MIT License](LICENSE).  
*(Include your license file in the repository.)*

## Acknowledgments

- Bureau of Industry and Security (BIS)
- Inter-American Development Bank (IDB)
- World Bank
- Australian Government
- European Bank for Reconstruction and Development (EBRD)
- African Development Bank (AfDB)
- Bank Negara Malaysia
- Open Sanctions Community

---

*Note: Replace placeholder links (`#`) with the actual URLs once available.*
