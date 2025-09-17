#!/usr/bin/env python3
"""
ODT Editor module for editing OpenDocument Text files
"""

from odf import text, teletype
from odf.opendocument import load, OpenDocumentText
from odf.table import Table, TableRow, TableCell
from odf.text import P
import os
import shutil


class ODTEditor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.doc = None
        self.tables = []
        
    def load(self):
        """Load the ODT document"""
        self.doc = load(self.file_path)
        self._extract_tables()
        
    def _extract_tables(self):
        """Extract all tables from the document"""
        self.tables = []
        for table in self.doc.getElementsByType(Table):
            table_obj = {'element': table, 'data': []}
            
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
                        row_data.append({'text': cell_text.strip(), 'cell': cell})
                
                if row_data:
                    table_obj['data'].append(row_data)
            
            self.tables.append(table_obj)
    
    def update_table_cell(self, table_index, row_index, col_index, new_text):
        """Update a specific cell in a table"""
        if table_index >= len(self.tables):
            raise IndexError("Table index out of range")
            
        table = self.tables[table_index]
        if row_index >= len(table['data']):
            raise IndexError("Row index out of range")
            
        row = table['data'][row_index]
        if col_index >= len(row):
            raise IndexError("Column index out of range")
            
        # Get the actual cell element
        cell_data = row[col_index]
        cell = cell_data['cell']
        
        # Clear existing content
        for p in cell.getElementsByType(text.P):
            cell.removeChild(p)
        
        # Add new content
        new_p = P(text=new_text)
        cell.addElement(new_p)
        
        # Update our data structure
        cell_data['text'] = new_text
    
    def update_table_from_csv_data(self, table_index, csv_data):
        """Update entire table from CSV-like data structure"""
        if table_index >= len(self.tables):
            raise IndexError("Table index out of range")
            
        for row_idx, row_data in enumerate(csv_data):
            for col_idx, cell_text in enumerate(row_data):
                try:
                    self.update_table_cell(table_index, row_idx, col_idx, cell_text)
                except IndexError:
                    # Skip cells that don't exist in the original table
                    pass
    
    def save(self, output_path=None):
        """Save the modified document"""
        if output_path is None:
            output_path = self.file_path
            
        # Create backup
        backup_path = self.file_path + '.bak'
        shutil.copy2(self.file_path, backup_path)
        
        # Save the document
        self.doc.save(output_path)
        
    def get_tables_as_lists(self):
        """Get all tables as list of lists"""
        result = []
        for table in self.tables:
            table_data = []
            for row in table['data']:
                row_data = [cell['text'] for cell in row]
                table_data.append(row_data)
            result.append(table_data)
        return result


def parse_csv_content(content):
    """Parse CSV-like content from textarea"""
    lines = content.strip().split('\n')
    table_data = []
    
    for line in lines:
        # Simple CSV parsing - could be enhanced with proper CSV library
        row = line.split('\t')  # Assuming tab-separated
        table_data.append(row)
    
    return table_data