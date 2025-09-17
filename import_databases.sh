#!/bin/bash

# Database Import Script for Phenology Project
# This script imports the pheno and pheno_new databases from SQL backup files

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Database configuration
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# SQL backup file paths (relative to script location)
PHENO_BACKUP="$SCRIPT_DIR/pheno_backup.sql"
PHENO_NEW_BACKUP="$SCRIPT_DIR/pheno_new_backup.sql"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Phenology Database Import Script${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Function to check if database exists
database_exists() {
    local db_name=$1
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -lqt | cut -d \| -f 1 | grep -qw $db_name
}

# Function to import database
import_database() {
    local db_name=$1
    local backup_file=$2
    
    echo -e "${YELLOW}Processing database: $db_name${NC}"
    
    # Check if backup file exists
    if [ ! -f "$backup_file" ]; then
        echo -e "${RED}Error: Backup file not found: $backup_file${NC}"
        return 1
    fi
    
    # Check if database already exists
    if database_exists $db_name; then
        echo -e "${YELLOW}Database $db_name already exists.${NC}"
        read -p "Do you want to drop and recreate it? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Dropping database $db_name..."
            dropdb -U $DB_USER -h $DB_HOST -p $DB_PORT $db_name
            if [ $? -ne 0 ]; then
                echo -e "${RED}Error: Failed to drop database $db_name${NC}"
                return 1
            fi
        else
            echo "Skipping $db_name..."
            return 0
        fi
    fi
    
    # Create database
    echo "Creating database $db_name..."
    createdb -U $DB_USER -h $DB_HOST -p $DB_PORT $db_name
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Failed to create database $db_name${NC}"
        return 1
    fi
    
    # Import data
    echo "Importing data into $db_name..."
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $db_name -f "$backup_file" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Successfully imported $db_name${NC}"
    else
        echo -e "${RED}Error: Failed to import data into $db_name${NC}"
        echo "Trying verbose mode to see errors..."
        psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $db_name -f "$backup_file"
        return 1
    fi
    
    return 0
}

# Main execution
echo "Starting database import process..."
echo ""

# Import pheno database
import_database "pheno" "$PHENO_BACKUP"
pheno_status=$?

echo ""

# Import pheno_new database
import_database "pheno_new" "$PHENO_NEW_BACKUP"
pheno_new_status=$?

echo ""
echo -e "${GREEN}========================================${NC}"

# Summary
if [ $pheno_status -eq 0 ] && [ $pheno_new_status -eq 0 ]; then
    echo -e "${GREEN}All databases imported successfully!${NC}"
    
    # Show database statistics
    echo ""
    echo "Database Statistics:"
    echo "-------------------"
    
    # pheno database stats
    echo -e "${YELLOW}pheno database:${NC}"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d pheno -t -c "SELECT COUNT(*) || ' observations' FROM dwd_observation;" 2>/dev/null || echo "Unable to get stats"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d pheno -t -c "SELECT COUNT(*) || ' stations' FROM dwd_station;" 2>/dev/null || echo "Unable to get stats"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d pheno -t -c "SELECT COUNT(*) || ' species' FROM dwd_species;" 2>/dev/null || echo "Unable to get stats"
    
    echo ""
    
    # pheno_new database stats
    echo -e "${YELLOW}pheno_new database:${NC}"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d pheno_new -t -c "SELECT COUNT(*) || ' observations' FROM dwd_observation;" 2>/dev/null || echo "Unable to get stats"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d pheno_new -t -c "SELECT COUNT(*) || ' stations' FROM dwd_station;" 2>/dev/null || echo "Unable to get stats"
    psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d pheno_new -t -c "SELECT COUNT(*) || ' species' FROM dwd_species;" 2>/dev/null || echo "Unable to get stats"
else
    echo -e "${RED}Some databases failed to import. Please check the errors above.${NC}"
    exit 1
fi

echo ""
echo "You can now run the Flask application with: python app.py"
echo -e "${GREEN}========================================${NC}"