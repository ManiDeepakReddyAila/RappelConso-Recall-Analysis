from diagrams import Diagram, Cluster, Edge
from diagrams.gcp.compute import Functions
from diagrams.gcp.analytics import BigQuery, PubSub, Dataproc
from diagrams.onprem.analytics import Spark

# Define the diagram
with Diagram("RappelConso Data Pipeline Architecture", show=False, direction="LR"):
    # Cloud Function for data ingestion
    with Cluster("Ingestion Layer"):
        cloud_function = Functions("Cloud Function")

    # Google Pub/Sub for message passing
    pubsub = PubSub("Google Pub/Sub")

    # Dataproc Cluster with PySpark for data processing
    with Cluster("Processing Layer"):
        dataproc = Dataproc("Dataproc Cluster")
        spark = Spark("PySpark Structured Streaming")

    # BigQuery for data storage and analytics
    bigquery = BigQuery("BigQuery")

    # Data Flow
    cloud_function >> Edge(label="Calls API") >> pubsub
    pubsub >> Edge(label="Publish data") >> dataproc
    dataproc >> Edge(label="Pull data") >> spark
    spark >> Edge(label="Transform and Push") >> bigquery
