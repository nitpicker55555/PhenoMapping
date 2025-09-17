#!/bin/bash

# Complete Setup Script for Phenology Project
# This script sets up the entire project including databases and Python dependencies

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Phenology Project Setup Script       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Step 1: Check Python version
echo -e "${YELLOW}Step 1: Checking Python environment...${NC}"
python_version=$(python3 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+')
echo "Python version: $python_version"

# Step 2: Install Python dependencies
echo ""
echo -e "${YELLOW}Step 2: Installing Python dependencies...${NC}"

# Create requirements.txt if it doesn't exist
if [ ! -f "$SCRIPT_DIR/requirements.txt" ]; then
    echo "Creating requirements.txt..."
    cat > "$SCRIPT_DIR/requirements.txt" << EOF
Flask==2.3.2
psycopg2-binary==2.9.9
pandas==2.0.3
odfpy==1.4.1
Pillow==10.0.0
EOF
fi

echo "Installing Python packages..."
pip3 install -r "$SCRIPT_DIR/requirements.txt"
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to install Python dependencies${NC}"
    echo "Please ensure pip3 is installed and try again."
    exit 1
fi

# Step 3: Check PostgreSQL
echo ""
echo -e "${YELLOW}Step 3: Checking PostgreSQL...${NC}"
if command -v psql &> /dev/null; then
    echo -e "${GREEN}PostgreSQL is installed${NC}"
    psql --version
else
    echo -e "${RED}PostgreSQL is not installed!${NC}"
    echo "Please install PostgreSQL and ensure the postgres user exists."
    echo "On macOS: brew install postgresql"
    echo "On Ubuntu/Debian: sudo apt-get install postgresql"
    exit 1
fi

# Step 4: Import databases
echo ""
echo -e "${YELLOW}Step 4: Importing databases...${NC}"
if [ -f "$SCRIPT_DIR/import_databases.sh" ]; then
    bash "$SCRIPT_DIR/import_databases.sh"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Database import failed!${NC}"
        exit 1
    fi
else
    echo -e "${RED}import_databases.sh not found!${NC}"
    exit 1
fi

# Step 5: Check if Transskriptionen folder exists
echo ""
echo -e "${YELLOW}Step 5: Checking data folder...${NC}"
if [ -d "$SCRIPT_DIR/Transskriptionen" ]; then
    echo -e "${GREEN}Transskriptionen folder found${NC}"
    folder_count=$(find "$SCRIPT_DIR/Transskriptionen" -maxdepth 1 -type d | wc -l)
    echo "Found $((folder_count - 1)) subfolders"
else
    echo -e "${RED}Warning: Transskriptionen folder not found!${NC}"
    echo "The transcription editor features will not work without this folder."
    echo "Please ensure the Transskriptionen folder is in: $SCRIPT_DIR/"
fi

# Step 6: Process phenology data (optional)
echo ""
read -p "Do you want to process phenology data from ODT files? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Processing phenology data...${NC}"
    python3 "$SCRIPT_DIR/phenology_data_processor.py"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Data processing completed${NC}"
        
        # Import to pheno_new database
        read -p "Do you want to import the processed data to pheno_new database? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            python3 "$SCRIPT_DIR/import_to_pheno_new.py"
        fi
    fi
fi

# Step 7: Create run script
echo ""
echo -e "${YELLOW}Step 7: Creating run script...${NC}"
cat > "$SCRIPT_DIR/run_app.sh" << 'EOF'
#!/bin/bash
# Run the Phenology Flask Application

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Starting Phenology Web Application..."
echo "Access the application at: http://localhost:9090"
echo "Press Ctrl+C to stop the server"
echo ""

python3 app.py
EOF

chmod +x "$SCRIPT_DIR/run_app.sh"

# Final summary
echo ""
echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║        Setup Complete!                 ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
echo ""
echo "Project is ready to run!"
echo ""
echo "To start the application:"
echo -e "${BLUE}  ./run_app.sh${NC}"
echo ""
echo "Or manually:"
echo -e "${BLUE}  python3 app.py${NC}"
echo ""
echo "The application will be available at:"
echo -e "${GREEN}  http://localhost:9090${NC}"
echo ""
echo "Available features:"
echo "  - View phenology observation data"
echo "  - Geographic visualization of stations"
echo "  - Species and phase analysis"
echo "  - Historical data comparison (pheno vs pheno_new)"
echo "  - Transcription editor for ODT files"
echo "  - New data locations on map"
echo ""