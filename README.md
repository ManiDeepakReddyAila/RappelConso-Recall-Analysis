# RappelConso Data Pipeline

RappelConso Data Pipeline is an end-to-end data processing solution designed to ingest, simulate streaming, transform, and analyze recall data from the French government’s RappelConso API. This project leverages Google Cloud’s scalable services, including Cloud Functions, Pub/Sub, Dataproc, and BigQuery, to deliver real-time insights and enable advanced analytics for product recalls.

## Tools and Technologies Used

![Python](https://img.shields.io/badge/Python-3.x-blue?logo=python&logoColor=white)
![Google Cloud Functions](https://img.shields.io/badge/Google%20Cloud%20Functions-4285F4?logo=google-cloud&logoColor=white)
![Google Dataproc](https://img.shields.io/badge/Google%20Dataproc-red?logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-green?logo=google-cloud&logoColor=white)
![Pub/Sub Lite](https://img.shields.io/badge/Pub/Sub%20Lite-orange?logo=google-cloud&logoColor=white)
![Google Cloud Translate](https://img.shields.io/badge/Google%20Cloud%20Translate-white?logo=google-cloud&logoColor=black)
![PySpark](https://img.shields.io/badge/PySpark-F9A03C?logo=apachespark&logoColor=white)

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Key Components](#key-components)
- [Technical Approach](#technical-approach)
- [Installation and Setup](#installation-and-setup)
- [Usage](#usage)

## Project Overview

RappelConso Data Pipeline automates the ingestion and simulation of streaming recall data from the RappelConso API, processes the data in real-time using PySpark on Dataproc, and stores the transformed data in BigQuery for further analysis. This architecture ensures that recall data is quickly available for querying and dashboarding, providing valuable insights to stakeholders.

## Architecture

The project follows a modular and scalable architecture designed for efficient data processing:

![RappelConso Data Pipeline Architecture](https://github.com/ManiDeepakReddyAila/RappelConso-Recall-Analysis/blob/master/rappelconso_data_pipeline_architecture.png)

1. **Cloud Function**: Ingests data by calling the RappelConso API and publishes simulated streaming messages to Google Pub/Sub.
2. **Google Pub/Sub**: Serves as a messaging layer, facilitating asynchronous communication between components.
3. **Dataproc with PySpark Structured Streaming**: Pulls data from Pub/Sub, applies complex transformations, and processes data in real-time.
4. **BigQuery**: Stores the transformed data, making it available for advanced analytics and dashboarding in tools like Google Looker.

## Key Components

- **Google Cloud Functions**: Used for ingesting data from external APIs and simulating streaming to Pub/Sub.
- **Google Pub/Sub**: A highly scalable messaging service to decouple components and simulate real-time data streams.
- **Google Cloud Dataproc**: Managed Spark cluster for processing large-scale data efficiently.
- **PySpark Structured Streaming**: For real-time data transformation, aggregation, and enrichment.
- **Google BigQuery**: Serverless data warehouse for storing and analyzing processed data.

## Technical Approach

1. **Simulated Streaming Ingestion with Cloud Functions**:
   - Cloud Function is triggered periodically to call the RappelConso API.
   - Data is fetched, cleaned, and published to a Google Pub/Sub topic, simulating a real-time streaming environment.

2. **Real-Time Data Processing with Dataproc and PySpark**:
   - A Dataproc cluster running PySpark Structured Streaming reads messages from the Pub/Sub topic.
   - The data undergoes complex transformations, including data cleaning, aggregation, enrichment, and risk categorization.
   - Multiple DataFrames are created to support different analytical views and KPIs for advanced dashboards.

3. **Data Storage and Analytics with BigQuery**:
   - Transformed data is written to BigQuery tables, enabling easy access and analysis.
   - The setup supports integration with data visualization tools like Google Looker for creating dynamic dashboards.

## Installation and Setup

### Prerequisites

- Google Cloud account with access to Cloud Functions, Pub/Sub, Dataproc, and BigQuery.
- Python 3.x and relevant Google Cloud SDK tools installed.
- BigQuery and Dataproc permissions set up for your service account.

### Steps

1. **Clone the repository:**

   ```bash
   git clone https://github.com/ManiDeepakReddyAila/RappelConso-Recall-Analysis.git
   cd RappelConso-DataPipeline

2. **Set up your environment:**
    Create a Google Cloud project and enable necessary APIs.
    Deploy the Cloud Function with the correct permissions to access Pub/Sub
 
3. **Deploy the Cloud Function:**
    Use console or CLI to deploy cloud functions

4. **Set up Dataproc Cluster:**
    Create a Dataproc cluster with the required BigQuery and Pub/Sub connectors

5. **Run the PySpark Streaming Job:**
    Submit pyspark job (transform.py) to the DataProc Cluster

## Usage
- **Simulated Streaming Data Ingestion:** The Cloud Function periodically calls the RappelConso API, ingests new data, and publishes it to Pub/Sub, simulating real-time streaming.
- **Real-time Processing:** Dataproc processes the streaming data, applies transformations, and writes it to BigQuery.
- **Analysis and Dashboards:** Use BigQuery and Looker to analyze the recall data and build insightful dashboards.

