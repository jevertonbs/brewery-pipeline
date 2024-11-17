# Brewery Pipeline Project

## Overview

The **Brewery Pipeline Project** is a data engineering solution built to consume data from the [Open Brewery DB API](https://www.openbrewerydb.org/), transform it through a Medallion Architecture (Bronze, Silver, Gold layers), and store it in a structured format for analysis. The project leverages tools like **Mage AI**, **Docker**, and **Python** for seamless ETL processing.

---

## Project Features

- **Bronze Layer**: Fetches raw data from the API and saves it in JSON format.
- **Silver Layer**: Cleans and transforms the data, saving it in Parquet format with partitioning by state and city.
- **Gold Layer**: Aggregates the data to provide insights like the number of breweries per type and location.
- **Scalable Design**: Implements a Medallion Architecture for easy scalability and adaptability.
- **Dockerized Setup**: Ensures consistent and reproducible environments.
- **Logging**: Tracks the pipeline execution and errors using `loguru`.

---

## Tech Stack

- **Python**: Core programming language for data processing.
- **Mage AI**: Low-code tool for building data pipelines.
- **Docker**: Containerization tool for environment management.
- **Pandas**: Data manipulation and analysis.
- **Loguru**: Advanced logging.
- **Parquet**: Efficient columnar storage format.

---

## Architecture Overview

1. **Bronze Layer**:
   - Fetches raw data from the Open Brewery DB API.
   - Saves the raw JSON data in `bronze_layer.json`.

2. **Silver Layer**:
   - Transforms and cleans the raw JSON data.
   - Renames columns, drops unnecessary data, and saves as partitioned Parquet files in the `silver_layer` directory.

3. **Gold Layer**:
   - Aggregates the cleaned data.
   - Calculates metrics such as the number of breweries by type and state.
   - Saves the aggregated data in Parquet format in the `gold_layer` directory.

---

## Setup Instructions

Prerequisites
Install Docker.
Install Git.

1. **Step 1: Clone the Repository**
git clone https://github.com/jevertonbs/brewery-pipeline.git
cd brewery-pipeline

2. **Step 2: Build and Run Docker Containers**
docker-compose build
docker-compose up -d

3. **Step 3: Set Up Mage AI**
Access Mage AI by opening http://localhost:6789 in your browser.
Navigate to the pipelines (mage) folder to set up or run the pipeline.

4. **Step 4: Run the Pipeline**
In Mage AI, run the pipeline stages:
Bronze Layer: Fetch raw data.
Silver Layer: Transform raw data.
Gold Layer: Generate aggregated data.

## Usage

**Logs**
Execution logs are saved in:

bronze_layer.log for Bronze Layer processing.
Other logs can be configured as needed.

**Output Data**
Bronze Layer: bronze_layer.json
Silver Layer: silver_layer/breweries_silver.parquet
Gold Layer: gold_layer/breweries_gold.parquet

## Testing
**Unit Tests**
Run the following command to execute unit tests:
pytest

**Sample Tests in Mage AI**
Bronze Layer: Ensures data is fetched and stored as JSON.
Silver Layer: Validates data transformation and column integrity.
Gold Layer: Checks aggregation logic and file creation.

## Known Issues and Troubleshooting

**Docker Container Issues:**
Ensure Docker is running.
Rebuild containers if necessary: docker-compose build.

**API Request Failures:**
Logs will indicate issues with API connectivity.
Check internet connection and API endpoint status.

**Partitioning Errors:**
Ensure Parquet dependencies (pyarrow) are installed.

**Future Improvements**
More tests
Implement data validation and schema enforcement.
Add CI/CD pipeline integration.
Introduce more advanced aggregations and metrics in the Gold Layer.

## Author
**José Everton Barreto da Silva**
[GitHub](https://github.com/jevertonbs) | [LinkedIn](https://www.linkedin.com/in/jevertonbs/)

## Feedback
For suggestions or issues, please create a GitHub issue or contact me directly.
