# Database Setup Guide

This guide explains how to set up the databases for the Phenology Mapping project.

## Database Overview

The project uses two PostgreSQL databases:

1. **pheno** - Main database with German Weather Service phenological observation data (1.4GB)
2. **pheno_new** - Historical phenology data from 1856 Bavaria transcriptions (306KB)

## Quick Setup

Run the complete setup script:
```bash
./setup_project.sh
```

This will:
- Install Python dependencies
- Import both databases
- Process phenology data (optional)
- Create a run script

## Manual Database Import

If you prefer to import databases manually:

```bash
# Import databases using the provided script
./import_databases.sh
```

Or manually with psql:

```bash
# Create and import pheno database
createdb -U postgres pheno
psql -U postgres -d pheno -f pheno_backup.sql

# Create and import pheno_new database  
createdb -U postgres pheno_new
psql -U postgres -d pheno_new -f pheno_new_backup.sql
```

## Database Structure

### pheno Database
Main tables:
- `dwd_observation` - Phenological observations
- `dwd_station` - Weather stations information
- `dwd_species` - Plant species data
- `dwd_phase` - Phenological phases
- `dwd_quality_level` - Data quality information

### pheno_new Database
Same structure as pheno, but contains:
- Historical observations from 1856
- Locations extracted from folder names
- Species mapped from historical German names

## Backup Files

- `pheno_backup.sql` - Full backup of pheno database
- `pheno_new_backup.sql` - Full backup of pheno_new database

## Requirements

- PostgreSQL 12+ 
- postgres user with database creation privileges
- Python 3.7+
- ~2GB free disk space

## Troubleshooting

### Database already exists error
The import script will ask if you want to drop and recreate existing databases.

### Permission denied
Ensure the postgres user has proper permissions:
```bash
sudo -u postgres psql
```

### Connection refused
Check if PostgreSQL is running:
```bash
# macOS
brew services start postgresql

# Linux
sudo service postgresql start
```

## Data Sources

- **pheno database**: German Weather Service (DWD) phenological observation network
- **pheno_new database**: Transcribed historical records from 1856 Bavaria