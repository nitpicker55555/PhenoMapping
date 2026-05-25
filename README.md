# Plant Phenology Observation Data Visualization Platform

An interactive visualization platform built on plant phenology observation data from the German Meteorological Service (DWD), showcasing 70 years of changes in plant growth cycles across Germany.

## Features

### 🏠 Data Overview
- Real-time statistics: 17M+ observation records, 1000+ observation stations, 97 plant species
- Interactive map previewing the distribution of observation stations across Germany
- Annual observation volume trend charts

### 🗺️ Geographic Distribution Analysis
- Interactive map displaying all observation stations
- Filter stations by state, elevation, and observation frequency
- Regional statistics charts and elevation distribution analysis
- Detailed station information tables

### 📈 Time Series Analysis
- Interannual trend analysis of phenological phases
- Anomalous year detection and climate impact assessment
- Custom species and phenological phase combination analysis
- Data export functionality

### 🌿 Species Research
- Detailed information for 97 plant species
- Species grouping statistics and phenological characteristic analysis
- Geographic distribution and observation statistics for each species
- Both grid and list browsing modes

### 🛡️ Data Quality Monitoring
- Three-tier quality level distribution statistics
- Annual data quality trend analysis
- Quality scoring and improvement suggestions

## Technical Architecture

### Backend
- **Flask**: Web framework
- **PostgreSQL**: Database
- **psycopg2**: Database connector

### Frontend
- **Bootstrap 5**: UI framework
- **Chart.js**: Chart visualization
- **Leaflet**: Map visualization
- **jQuery**: JavaScript library

### Data Structure
The database contains 8 main tables:
- `dwd_observation`: Core observation data table (17M+ records)
- `dwd_station`: Observation station information
- `dwd_species`: Plant species information
- `dwd_phase`: Phenological phase definitions
- `dwd_quality_level`: Data quality levels
- `dwd_quality_byte`: Quality byte codes
- `dwd_species_group`: Species groupings
- `dwd_about`: Dataset metadata

## Installation and Deployment

### Requirements
- Python 3.8+
- PostgreSQL 12+
- Modern web browser

### 1. Clone the Project
```bash
git clone <repository-url>
cd PhenoMapping
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Database Configuration
Ensure the PostgreSQL service is running and that a database named `pheno` exists containing the DWD phenology observation data.

Modify the database configuration in `app.py`:
```python
DB_CONFIG = {
    'host': 'localhost',
    'database': 'pheno', 
    'user': 'postgres',
    'password': 'your_password',
    'port': '5432'
}
```

### 4. Run the Application
```bash
python app.py
```

Visit `http://localhost:5000` to view the application.

## API Endpoints

### Main API Endpoints
- `GET /api/overview` - Data overview statistics
- `GET /api/stations` - Observation station list
- `GET /api/species` - Plant species information
- `GET /api/phases` - Phenological phase information
- `GET /api/observations` - Observation data (supports filtering)
- `GET /api/trends` - Trend analysis data
- `GET /api/quality` - Data quality statistics

### Filter Parameters
The observations endpoint supports the following filter parameters:
- `station_id`: Station ID
- `species_id`: Species ID
- `phase_id`: Phenological phase ID
- `year_start`: Start year
- `year_end`: End year
- `limit`: Limit on number of records returned

## Data Source

This project uses publicly available plant phenology observation data from the German Meteorological Service (DWD):
- **Data Source**: DWD Climate Data Center (CDC)
- **Time Span**: 1953 to present
- **Observation Content**: Plant budding, flowering, fruiting, leaf-fall, and other phenological phases
- **Spatial Coverage**: 1000+ observation stations across Germany

## License

This project is licensed under the MIT License. The data comes from the German Meteorological Service and follows its open data policy.

## Contributing

Issues and Pull Requests are welcome to help improve this project.

## Contact

For questions or suggestions, please reach out via GitHub Issues.
