#!/usr/bin/env python3
"""
Phenology Data Processor
Processes ODT files from Transskriptionen folders and merges phenological observation data into a single CSV file.
"""

import os
import sys
import re
import csv
from pathlib import Path
from odf import text, teletype
from odf.opendocument import load
from odf.table import Table, TableColumn, TableRow, TableCell

def extract_tables_from_odt(odt_file_path):
    """Extract all tables from an ODT file"""
    try:
        doc = load(odt_file_path)
        tables = []
        
        for table in doc.getElementsByType(Table):
            table_data = []
            
            for row in table.getElementsByType(TableRow):
                row_data = []
                
                for cell in row.getElementsByType(TableCell):
                    cell_text = ""
                    for p in cell.getElementsByType(text.P):
                        cell_text += teletype.extractText(p)
                    
                    repeat = cell.getAttribute("numbercolumnsrepeated")
                    if repeat:
                        repeat_count = int(repeat)
                    else:
                        repeat_count = 1
                    
                    for _ in range(repeat_count):
                        row_data.append(cell_text.strip())
                
                if row_data:
                    table_data.append(row_data)
            
            if table_data:
                tables.append(table_data)
        
        return tables
    except Exception as e:
        print(f"Error processing {odt_file_path}: {str(e)}")
        return []

def save_table_as_csv(table_data, csv_file_path):
    """Save table data as CSV file"""
    try:
        max_cols = max(len(row) for row in table_data) if table_data else 0
        
        padded_data = []
        for row in table_data:
            padded_row = row + [''] * (max_cols - len(row))
            padded_data.append(padded_row)
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(padded_data)
        
        return True
    except Exception as e:
        print(f"Error saving CSV {csv_file_path}: {str(e)}")
        return False

def extract_folder_index(file_path):
    """Extract the folder index (leading numbers) from the file path"""
    parent_dir = os.path.basename(os.path.dirname(file_path))
    match = re.match(r'^(\d+)', parent_dir)
    if match:
        return match.group(1)
    return None

def find_odt_files_in_tabelle_folders(base_dir):
    """Find all ODT files in folders containing 'Tabelle' in their name"""
    odt_files = []
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(base_dir):
        # Check if current directory name contains "Tabelle" or "tabelle"
        if 'tabelle' in os.path.basename(root).lower():
            for file in files:
                if file.endswith('.odt'):
                    odt_files.append(os.path.join(root, file))
    
    # Also check for the special case folder "43 - 1856 Allersberg - Taeger"
    special_folder = os.path.join(base_dir, "43 - 1856 Allersberg - Taeger")
    if os.path.exists(special_folder):
        for file in os.listdir(special_folder):
            if file.endswith('.odt') and file.startswith('Tabelle'):
                odt_files.append(os.path.join(special_folder, file))
    
    return sorted(odt_files)

def process_odt_files(base_dir, output_dir):
    """Process all ODT files and extract tables to CSV"""
    # Find all ODT files
    odt_files = find_odt_files_in_tabelle_folders(base_dir)
    print(f"Found {len(odt_files)} ODT files to process\n")
    
    # Process each ODT file
    for odt_file in odt_files:
        print(f"Processing: {odt_file}")
        
        # Extract tables
        tables = extract_tables_from_odt(odt_file)
        
        if not tables:
            print(f"  No tables found in {odt_file}")
            continue
        
        # Generate base filename for CSV
        base_name = os.path.splitext(os.path.basename(odt_file))[0]
        folder_index = extract_folder_index(odt_file)
        
        # Save each table as a separate CSV
        for i, table in enumerate(tables):
            if folder_index:
                if len(tables) == 1:
                    csv_filename = f"{folder_index}_{base_name}.csv"
                else:
                    csv_filename = f"{folder_index}_{base_name}_table_{i+1}.csv"
            else:
                if len(tables) == 1:
                    csv_filename = f"{base_name}.csv"
                else:
                    csv_filename = f"{base_name}_table_{i+1}.csv"
            
            csv_path = os.path.join(output_dir, csv_filename)
            
            if save_table_as_csv(table, csv_path):
                print(f"  Saved table {i+1} to: {csv_filename}")
            else:
                print(f"  Failed to save table {i+1}")

def extract_index_from_filename(filename):
    """Extract the index number from the beginning of the filename"""
    match = re.match(r'^(\d+)_', filename)
    if match:
        return match.group(1)
    return None

def merge_16column_tables(csv_dir, output_file):
    """Merge all 16-column tables into a single CSV file"""
    # Headers from the standard phenology observation table
    first_row_headers = ["Name der Gewächse", "Blätter", "Blüthen", "Früchte", "Genaue Bezeichnung der Standorte"]
    second_row_headers = [
        "Die Knospen brechen.",
        "Die ersten Blätter sind entfaltet.",
        "Allgemeine Belaubung.",
        "Die ersten Blätter zeigen die farbliche Färbung.",
        "Alle Blätter zeigen die farbliche Färbung.",
        "Das abfallen der Blätter beginnt.",
        "Alle Blätter sind abgefallen.",
        "Die ersten Blüthen sind entfaltet.",
        "Allgemeines Blühen.",
        "Sämtliche Blüthen sind verblüht.",
        "Dauer einer einzelnen Blüthe von der Entfaltung bis zum Verblühen.",
        "Die ersten Früchte sind reif.",
        "Allgemeine Fruchtreife.",
        "Sämtliche Früchte sind abgefallen."
    ]
    
    # Construct final headers
    final_headers = ["Index", first_row_headers[0]] + second_row_headers + [first_row_headers[4]]
    
    # Collect all 16-column tables
    tables_to_merge = []
    
    for filename in sorted(os.listdir(csv_dir)):
        if filename.endswith('.csv') and '_table_' in filename:
            filepath = os.path.join(csv_dir, filename)
            
            # Check if this is a 16-column table
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                first_row = next(reader, None)
                if first_row and len(first_row) == 16:
                    index = extract_index_from_filename(filename)
                    if index:
                        tables_to_merge.append({
                            'filename': filename,
                            'filepath': filepath,
                            'index': index
                        })
    
    print(f"\nFound {len(tables_to_merge)} 16-column tables to merge")
    
    # Write merged CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(final_headers)
        
        for table_info in tables_to_merge:
            print(f"Merging: {table_info['filename']}")
            
            with open(table_info['filepath'], 'r', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                
                for row in reader:
                    if row and len(row) == 16:
                        new_row = [table_info['index']] + row
                        writer.writerow(new_row)
    
    print(f"\nMerged data saved to: {output_file}")
    
    # Count total rows
    with open(output_file, 'r', encoding='utf-8') as f:
        row_count = sum(1 for line in f) - 1
    
    print(f"Total data rows: {row_count}")

def main():
    """Main function to process phenology data"""
    if len(sys.argv) > 1:
        base_dir = sys.argv[1]
    else:
        base_dir = "/Users/puzhen/Downloads/Transskriptionen"
    
    # Validate input directory
    if not os.path.exists(base_dir):
        print(f"Error: Directory '{base_dir}' does not exist")
        sys.exit(1)
    
    print(f"Processing phenology data from: {base_dir}")
    
    # Create output directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_output_dir = os.path.join(script_dir, "extracted_tables_csv")
    os.makedirs(csv_output_dir, exist_ok=True)
    
    # Step 1: Extract tables from ODT files
    print("\n=== Step 1: Extracting tables from ODT files ===")
    process_odt_files(base_dir, csv_output_dir)
    
    # Step 2: Merge 16-column tables
    print("\n=== Step 2: Merging 16-column tables ===")
    merged_output_file = os.path.join(script_dir, "merged_phenology_data.csv")
    merge_16column_tables(csv_output_dir, merged_output_file)
    
    print("\n=== Processing complete! ===")
    print(f"Individual CSV files: {csv_output_dir}")
    print(f"Merged data file: {merged_output_file}")

if __name__ == "__main__":
    main()