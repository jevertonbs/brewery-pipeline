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
Start docker
docker-compose build

![{result-for-docker-compose-build}](https://github.com/user-attachments/assets/07f19828-f2ab-48e3-9f0b-f18b7d4918b5)
This is ok

docker-compose up -d

![result-for-docker-compose-up-d}](https://github.com/user-attachments/assets/d28e9b98-6b52-4e42-840d-7a0c95645dc0)
This is ok

4. **Step 3: Set Up Mage AI**
Access Mage AI by opening http://localhost:6789 in your browser.
![{Home}](https://github.com/user-attachments/assets/ed3ef8b0-1110-4522-a9e5-066084b27afd)


Navigate to the pipelines (mage) folder to set up or run the pipeline.
![pipeline](https://github.com/user-attachments/assets/526e0365-1727-4bb6-8db3-d7a6258136de)

Enter in pipeline brewery
![brewery](https://github.com/user-attachments/assets/fca587aa-e4c5-4e36-967a-9b77ebd1d409)

Click in run once 
![run](https://github.com/user-attachments/assets/cf12c46a-f749-4f2c-862b-001b2104a326)

After in Run now and wait
![{run-now}](https://github.com/user-attachments/assets/4f4c3c2d-e3ac-43ea-a796-8a0e54540a6c)

There is already a trigger that runs every day at midnight.

In edit pipeline you can see the code and see a preview of the data
![edit-pipeline](https://github.com/user-attachments/assets/1cccce3f-0ab6-41c0-b4f3-9a895d886554)

5. **Step 4: Run the Pipeline**
In Mage AI, run the pipeline stages:
Bronze Layer: Fetch raw data.
![{Bronze-Layer}](https://github.com/user-attachments/assets/7fd342d1-ccd2-43f0-b11c-843f40ace66a)

Silver Layer: Transform raw data.
![Silver-Layer](https://github.com/user-attachments/assets/c2d72035-5424-4e3d-8806-977c57b1feda)

Gold Layer: Generate aggregated data.
![{Gold-Layer}](https://github.com/user-attachments/assets/abc17b9a-9963-456a-a674-0fd65fe73a3d)


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

## Future Improvements

**Pipeline Failure Monitoring:**
Configure Mage's logging system to track pipeline execution and errors effectively.
Integrate Loguru for detailed logging and set up email or Slack notifications for failure alerts.
Explore external monitoring tools like Prometheus or Docker health checks for enhanced oversight.

**Data Quality Checks:**
Add validation blocks within the pipeline to enforce schema consistency, check for null values, and eliminate duplicates.
Define acceptable data quality thresholds and implement logging to capture anomalies.
Automate alert triggers when data issues arise to ensure timely resolution.

**Alerting System Implementation:**
Use tools like PagerDuty or OpsGenie for centralized alert management.
Set up real-time notifications for critical events and integrate them with Slack or similar platforms.
Define escalation workflows to ensure timely responses to issues.

**Dashboards and Visualization:**
Implement Grafana for monitoring pipeline performance and resource utilization by connecting it to the Mage logs or Prometheus metrics.
Build business-focused dashboards in Looker to provide insights into data quality, pipeline execution metrics, and final output statistics.
Create automated reports and shareable dashboards for stakeholders to track key metrics.

**Integration with Google Cloud Platform (GCP):**
Store pipeline outputs in GCP services such as BigQuery, Google Cloud Storage, or S3.
Use GCP's monitoring tools, like Cloud Monitoring, to set alerts and track resource usage.
Integrate Looker with GCP datasets for seamless visualization of data stored in BigQuery.
Ensure secure and efficient connections between Mage and GCP for processing and analysis.

## Author
**José Everton Barreto da Silva**
[GitHub](https://github.com/jevertonbs) | [LinkedIn](https://www.linkedin.com/in/jevertonbs/)

## Feedback
For suggestions or issues, please create a GitHub issue or contact me directly.
