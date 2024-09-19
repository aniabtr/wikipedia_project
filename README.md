## Overview

This project demonstrates an end-to-end data engineering pipeline for football data analytics, utilizing various technologies and services. The project encompasses data extraction, processing, storage, analysis, and visualization.

## Key Steps and Components

### Part 1: Data Extraction and Processing

1. **Set Up Project Infrastructure**:
   - Utilize Apache Airflow and Docker to set up the project environment.

2. **Create a DAG**:
   - Develop a Directed Acyclic Graph (DAG) in Airflow to orchestrate the data pipeline.

3. **Web Scraping**:
   - **Data Source Selection**: Identify relevant Wikipedia pages containing football stadium data.
   - **HTTP Requests**: Use Python's `requests` library to send GET requests to the URLs.
   - **HTML Parsing**: Employ Beautiful Soup to parse HTML content and extract data from tables.
   - **Data Extraction**: Gather specific information such as stadium names, locations, and capacities.
   - **Geolocation Enrichment**: Utilize the `geopy` library to convert stadium locations into latitude and longitude coordinates.
   - **Data Cleaning and Formatting**: Clean extracted data to remove HTML artifacts and ensure consistency.
   - **Error Handling**: Implement error management for missing data or network issues.
   - **Rate Limiting**: Incorporate rate limiting to avoid overwhelming Wikipedia's servers.
   - **Data Validation**: Perform checks to ensure accuracy and completeness of the scraped data.
   - **Output Generation**: Save the cleaned data as a CSV file for further processing.

4. **Transform Data**:
   - Structure the cleaned data into a suitable format and save it as a CSV file.

### Part 2: Azure Integration

1. **Set Up Azure Resources**:
   - Create a Resource Group, Storage Account (with Data Lake Gen2), Data Factory, and Synapse Analytics workspace.

2. **Create Container in Azure Data Lake Storage**.

3. **Set Up Azure Data Factory Pipeline**:
   - Move data from the local environment to Azure Data Lake Storage.

4. **Create Datasets in Azure Data Factory**:
   - Define datasets for source and destination data.

5. **Use Copy Activity in Data Factory Pipeline**:
   - Transfer data from source to destination.

6. **Set Up Synapse Analytics**:
   - Create a Serverless SQL pool and an external table for querying data directly from the Data Lake.

7. **Analyze Data with SQL Queries in Synapse Analytics**.

### Part 3: Data Visualization with Power BI

1. **Connect Power BI to Azure Synapse Analytics**.
   
2. **Import Football Stadium Data into Power BI**.

3. **Create Data Models and Relationships in Power BI**.

4. **Design Visualizations**:
   - Map showing stadium locations.
   - Bar charts for stadium capacities.
   - Filters for countries and regions.

5. **Create Interactive Dashboard Combining Multiple Visualizations**.

6. **Implement Drill-Through Functionality for Detailed Stadium Information**.

7. **Add Conditional Formatting to Highlight Specific Data Points**.

8. **Publish Power BI Report to Power BI Service for Sharing and Collaboration**.

## Conclusion

This end-to-end project showcases a complete data engineering pipeline, from data extraction using web scraping with Apache Airflow, through data storage and analysis in Azure services, to final visualization in Power BI. It highlights skills in Python programming, web scraping, data cleaning, cloud computing with Azure, and data visualization.
