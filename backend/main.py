from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pathlib import Path
import math

app = FastAPI(title="Dashboard API")

# Add CORS headers so the frontend can call the API from a different port
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins for local development
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Resolve the path to the processed data folder
# __file__ is backend/main.py, so its parent is 'backend', and its parent is the root project folder
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"

def load_csv_data(filename: str):
    """Helper function to load CSV and convert to JSON-friendly dictionary."""
    file_path = DATA_DIR / filename
    
    if not file_path.exists():
        # Return HTTP 404 with a message if the data file is not found
        raise HTTPException(status_code=404, detail=f"Data file '{filename}' not found.")
        
    try:
        df = pd.read_csv(file_path)
        # Convert NaN values to None (which becomes null in JSON) to avoid JSON serialization errors
        df = df.replace({float('nan'): None})
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "ok"}

@app.get("/api/revenue")
def get_revenue():
    """Returns monthly revenue data."""
    return load_csv_data("monthly_revenue.csv")

@app.get("/api/top-customers")
def get_top_customers():
    """Returns the top 10 customers list."""
    return load_csv_data("top_customers.csv")

@app.get("/api/categories")
def get_categories():
    """Returns category performance data."""
    return load_csv_data("category_performance.csv")

@app.get("/api/regions")
def get_regions():
    """Returns regional analysis data."""
    return load_csv_data("regional_analysis.csv")