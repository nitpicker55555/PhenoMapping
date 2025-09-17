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

def extract_date_from_folder_name(folder_path):
    """Extract date from folder name (e.g., '25.11.1856' from '6 Tabelle - Freihöls 25.22.1856 - Gigglberger')"""
    folder_name = os.path.basename(folder_path)
    
    # Try different date patterns
    patterns = [
        r'(\d{1,2}\.\d{1,2}\.\d{4})',  # DD.MM.YYYY
        r'(\d{1,2}\.\d{1,2}\.\d{3,4})',  # DD.MM.YYY or DD.MM.YYYY (for typos like 2856)
        r'(\d{4})',  # Just year
    ]
    
    for pattern in patterns:
        match = re.search(pattern, folder_name)
        if match:
            date_str = match.group(1)
            # Fix common typos (e.g., 2856 -> 1856)
            date_str = date_str.replace('2856', '1856')
            # Fix typos like 25.22.1856 -> 25.11.1856
            date_str = re.sub(r'(\d{1,2})\.22\.(\d{4})', r'\1.11.\2', date_str)
            return date_str
    
    return None

def extract_location_from_folder_name(folder_path):
    """Extract location name from folder path using simplified method"""
    folder_name = os.path.basename(folder_path)
    
    # First check for special cases
    if 'unbestimmt' in folder_name.lower():
        return 'Unknown'
    
    # Check for patterns indicating no location
    # Pattern 1: "number Tabelle - - person"
    if re.match(r'^\d+\s+Tabelle\s*-\s*-\s*\w+', folder_name):
        return 'Unknown'
    # Pattern 2: "number Tabelle - date - person" (no location)
    if re.match(r'^\d+\s+Tabelle\s*-\s*\d+\.\d+\.\d+\s*-\s*\w+', folder_name):
        return 'Unknown'
    # Pattern 3: "number Tabelle - year - person"  
    if re.match(r'^\d+\s+Tabelle\s*-\s*\d{4}\s*-\s*\w+$', folder_name):
        return 'Unknown'
    
    # Remove file extensions if present
    folder_name = re.sub(r'\.(odt|csv|pdf)$', '', folder_name, flags=re.IGNORECASE)
    
    # Remove 'Tabelle' (case-insensitive)
    cleaned = re.sub(r'\bTabelle\b', '', folder_name, flags=re.IGNORECASE)
    
    # Remove leading numbers and following spaces/dashes
    cleaned = re.sub(r'^\d+\s*[-\s]*', '', cleaned)
    
    # Remove dates (patterns like 12.11.1856, 1856, etc.)
    cleaned = re.sub(r'\d{1,2}\.\d{1,2}\.\d{4}', '', cleaned)
    cleaned = re.sub(r'\b\d{4}\b', '', cleaned)
    
    # Split by dash to separate location from person name
    # Pattern: "location - person" or "- location - person" or "date location - person"
    parts = cleaned.split('-')
    
    if len(parts) >= 2:
        # Try to find the location part (usually before the last dash)
        for i, part in enumerate(parts[:-1]):
            part = part.strip()
            # Skip empty parts
            if not part:
                continue
            # Skip if it's just a date remainder
            if re.match(r'^\s*\.?\s*$', part):
                continue
            # Check if this looks like a location (starts with capital letter, not too long)
            # Special words that are known locations or descriptors
            location_words = ['Bemerkungen', 'Freihöls', 'Freudenberg', 'Sulzbach', 
                            'Taubenbach', 'Kastl', 'Wernberg', 'Berg', 'Richtheim',
                            'Hilpoltstein', 'Allersberg']
            words_in_part = part.split()
            if words_in_part and (words_in_part[0] in location_words or 
                                (words_in_part[0][0].isupper() and len(words_in_part) <= 2)):
                # Remove FR prefix if present
                location = re.sub(r'^FR\s+', '', part)
                return location.strip()
    
    # If no pattern matched, try to extract first capitalized word(s)
    words = cleaned.split()
    location_words = []
    for word in words:
        word = word.strip()
        if word and word[0].isupper() and len(word) > 1:
            # Remove FR prefix
            if word == 'FR' and location_words:
                continue
            location_words.append(word)
            # Stop after getting 1-2 location words
            if len(location_words) >= 2:
                break
        # Stop if we hit a lowercase word or a surname pattern
        elif location_words:
            break
    
    if location_words:
        return ' '.join(location_words)
    
    return 'Unknown'

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
    
    # Create mappings for folder metadata
    folder_date_mapping = {}
    folder_location_mapping = {}
    
    # Process each ODT file
    for odt_file in odt_files:
        print(f"Processing: {odt_file}")
        
        # Extract folder information
        folder_path = os.path.dirname(odt_file)
        folder_index = extract_folder_index(odt_file)
        folder_date = extract_date_from_folder_name(folder_path)
        folder_location = extract_location_from_folder_name(folder_path)
        
        if folder_index:
            if folder_date:
                folder_date_mapping[folder_index] = folder_date
                print(f"  Folder {folder_index}: Date = {folder_date}")
            if folder_location:
                folder_location_mapping[folder_index] = folder_location
                print(f"  Folder {folder_index}: Location = {folder_location}")
        
        # Extract tables
        tables = extract_tables_from_odt(odt_file)
        
        if not tables:
            print(f"  No tables found in {odt_file}")
            continue
        
        # Generate base filename for CSV
        base_name = os.path.splitext(os.path.basename(odt_file))[0]
        
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
    
    # Return both mappings for later use
    return folder_date_mapping, folder_location_mapping

def extract_index_from_filename(filename):
    """Extract the index number from the beginning of the filename"""
    match = re.match(r'^(\d+)_', filename)
    if match:
        return match.group(1)
    return None

def merge_16column_tables(csv_dir, output_file, folder_date_mapping=None, folder_location_mapping=None):
    """Merge all 16-column tables into a single CSV file with date and location information"""
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
    
    # Construct final headers with date and location columns
    final_headers = ["Index", "Date", "Location", first_row_headers[0]] + second_row_headers + [first_row_headers[4]]
    
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
            
            # Get date and location for this index
            date = ""
            location = "Unknown"  # Default to Unknown
            if folder_date_mapping and table_info['index'] in folder_date_mapping:
                date = folder_date_mapping[table_info['index']]
            if folder_location_mapping and table_info['index'] in folder_location_mapping:
                location = folder_location_mapping[table_info['index']]
                if not location:  # Handle None or empty string
                    location = "Unknown"
            
            with open(table_info['filepath'], 'r', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                
                for row in reader:
                    if row and len(row) == 16:
                        new_row = [table_info['index'], date, location] + row
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
        # Use relative path to Transskriptionen folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.join(script_dir, "Transskriptionen")
    
    # Validate input directory
    if not os.path.exists(base_dir):
        print(f"Error: Directory '{base_dir}' does not exist")
        sys.exit(1)
    
    print(f"Processing phenology data from: {base_dir}")
    
    # Create output directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_output_dir = os.path.join(script_dir, "extracted_tables_csv")
    os.makedirs(csv_output_dir, exist_ok=True)
    
    # Step 1: Extract tables from ODT files and get date and location mappings
    print("\n=== Step 1: Extracting tables from ODT files ===")
    folder_date_mapping, folder_location_mapping = process_odt_files(base_dir, csv_output_dir)
    
    # Step 2: Merge 16-column tables with date and location information
    print("\n=== Step 2: Merging 16-column tables with date and location information ===")
    merged_output_file = os.path.join(script_dir, "merged_phenology_data.csv")
    merge_16column_tables(csv_output_dir, merged_output_file, folder_date_mapping, folder_location_mapping)
    
    print("\n=== Processing complete! ===")
    print(f"Individual CSV files: {csv_output_dir}")
    print(f"Merged data file: {merged_output_file}")

if __name__ == "__main__":
    main()