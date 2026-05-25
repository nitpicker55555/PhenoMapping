# Phenology Data Processor

This program processes phenological observation data by extracting tables from ODT files and merging them into a single CSV file.

## Features

1. Automatically finds ODT files in all folders containing "Tabelle"
2. Extracts all tables from each ODT file
3. Saves tables as individual CSV files, with filenames that include the source folder's index number
4. Automatically identifies and merges all standard 16-column phenological observation tables
5. Generates a final merged CSV file containing all observation data

## Usage

### Method 1: Use the Simple Wrapper Script (Recommended)
```bash
python3 process_phenology.py
```

Or specify an input directory:
```bash
python3 process_phenology.py ``/Users/puzhen/Downloads/Transskriptionen``
```

### Method 2: Run the Main Program Directly
```bash
python3 phenology_data_processor.py
```

## Output Files

The program creates the following files and folders in the current directory:

1. `extracted_tables_csv/` - Contains all individual CSV files extracted from ODT files
2. `merged_phenology_data.csv` - The final merged CSV file

## Structure of the Merged CSV File

The final CSV file contains 17 columns:

1. **Index** - Index number of the source folder
2. **Name der Gewächse** - Plant name
3-16. 14 date columns for phenological observation stages
17. **Genaue Bezeichnung der Standorte** - Observation location description

## Notes

- The program automatically installs the required Python packages (odfpy and pandas)
- Ensure the input directory path is correct
- The program skips folders that contain no ODT files
- Only 16-column tables are merged into the final file
- 2-column note tables are not included in the merged file

## Dependencies

- Python 3.x
- odfpy
- pandas (installed automatically)
