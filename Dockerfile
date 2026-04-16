# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (needed for pyodbc/msal if applicable)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Revenue Intelligence Space (test1 workspace, ZL model)
ENV DATABASE_BACKEND=powerbi
ENV FABRIC_WORKSPACE_NAME=test1
ENV WS_REVENUE_ID=2519f7d8-058d-4795-8d70-e29b045f92eb
ENV MODEL_REVENUE_ID=bca8f683-c1a6-4842-b877-3282cdcf391a
ENV AZURE_TENANT_ID=aca0b239-69e9-4246-87ba-8e07ad0a9249
ENV AZURE_CLIENT_SECRET=__SET_AT_RUNTIME__

# Schema mapping — Fact_Sales_Detail has Date, Site, Barcode, DEPARTMENT, SECTION, Discount %, Return Flag
ENV PBI_TABLE_SALES=Fact_Sales_Detail
ENV PBI_TABLE_PURCHASES=purchase_data
ENV PBI_TABLE_INVENTORY=Fact_Stock_Received
ENV PBI_TABLE_STORES=Dim_SKU_Store_Season
ENV PBI_TABLE_PRODUCTS=item_master
# Fact_Sales_Detail columns (use exact names from Model view)
ENV PBI_COL_SALE_DATE=Date
ENV PBI_COL_SALE_SITE=Site
ENV PBI_COL_SALE_BARCODE=Barcode
ENV PBI_COL_SALE_IS_RETURN=Return_Flag
ENV PBI_SALE_RETURN_VALUE_YES=Return
ENV PBI_COL_SALE_DISCOUNT="Discount %"
# Category from sales table
ENV PBI_COL_SALE_DEPT=DEPARTMENT
ENV PBI_COL_SALE_SECTION=SECTION
# Amount/Qty — columns have date range in name; copy EXACT name from Model view
# Examples from your model: Net Sale Qty01/11/2023-26/01/20..., Net Sale Amt01/11/2023-26/01/2026
ENV PBI_COL_SALE_AMOUNT="Net Sale Amt01/11/2023-26/01/2026"
ENV PBI_COL_SALE_QTY="Net Sale Qty01/11/2023-26/01/2026"
# purchase_data: Site_Code, Barcode, Purchase Qty (no date column)
ENV PBI_COL_PURCH_SITE=Site_Code
ENV PBI_COL_PURCH_BARCODE=Barcode
ENV PBI_COL_PURCH_QTY="Purchase Qty01/11/2023-26/01/2026"
ENV PBI_PURCH_HAS_DATE=false
# If your purchases table has a date column, set PBI_PURCH_HAS_DATE=true and:
# PBI_COL_PURCH_DATE=Date
# Fact_Stock_Received: Date, Site, SKU, Purchase Qty
ENV PBI_COL_INV_DATE=Date
ENV PBI_COL_INV_SITE=Site
ENV PBI_COL_INV_BARCODE=SKU
ENV PBI_COL_INV_QTY=Purchase_Qty
# Dim_SKU_Store_Season: Site, Season
ENV PBI_COL_STORE_SITE=Site
ENV PBI_COL_STORE_ZONE=Season
# item_master: BARCODE, DEPARTMENT, DIVISION
ENV PBI_COL_PROD_BARCODE=BARCODE
ENV PBI_COL_PROD_DEPT=DEPARTMENT
ENV PBI_COL_PROD_SECTION=DIVISION

# Expose port (FastAPI default in this project seems to be 5001)
EXPOSE 5001

# Command to run the application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5001"]
