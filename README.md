# Health Data Streaming Pipeline

This project is a real-time data streaming pipeline for healthcare data, using Apache Kafka, Apache Spark Structured Streaming, and Amazon S3 for storage. The pipeline ingests simulated patient data from a Kafka topic, processes it with Apache Spark, and stores the processed data in Apache Parquet format on an Amazon S3 bucket.

## Components

1. **Data Producer** (`producer.py`): A Python script that generates mock patient data and publishes it to a Kafka topic.
2. **Data Consumer** (`storage.py`): A PySpark script that reads data from the Kafka topic, processes it, and writes the processed data to an Amazon S3 bucket in Parquet format.
3. **Data Analysis** (`read_S3.ipynb`): A file that read and analyze the healthcare data stored in the Parquet files on Amazon S3. 

## Prerequisites

- Python 3.11.7
- Apache Kafka
- Apache Spark
- Python packages: `kafka-python`, `faker`, `pyspark`
- AWS credentials with access to an S3 bucket
- Jupyter Notebook

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yousrimz/healthstream.git
   cd healthstream
   ```

2. Install the required Python packages:

    ```bash
    pip install kafka-python pyspark faker
    ```

3. Configure AWS credentials by creating a ***config.json*** file in the utils folder with the following structure:

    ```json
    {
  "aws_access_key_id": "your-aws-access-key-id",
  "aws_secret_access_key": "your-aws-secret-access-key"
    }
    
    ```

4. Start the Kafka server and create the **arhvdata** topic:
    ```bash
    kafka-server-start.sh /path/to/kafka/config/server.properties
    ```
    ```bash
    kafka-topics.sh --create --topic arhvdata --bootstrap-server localhost:9092
    ```

## Usage

1. Start the data producer:
    ```bash
    python producer.py
    ```
    This action will generate mock patient data and publish it to the Kafka topic ***arhvdata***.

2. In a separate terminal, start the data consumer (Spark Structured Streaming):
    ```bash
    python storage.py
    ```
    This will start the Spark Structured Streaming job, which will read data from the Kafka topic and write it to the specified Amazon S3 bucket in Parquet format.

3. To read and display the data stored in the S3 bucket, open the notebooks/read_S3.ipynb notebook in Jupyter Notebook and run the cells.

## Monitoring

You can monitor the progress of the Spark Structured Streaming job by checking the Spark UI at http://localhost:4040. Additionally, you can check the Amazon S3 bucket to verify that the data is being written correctly.

## Data Analysis with Jupyter Notebook

This project also includes a Jupyter Notebook (`notebooks/read_S3.ipynb`) that allows you to read and analyze the healthcare data stored in the Parquet files on Amazon S3. The notebook provides the following features:

- Reading Parquet files from the S3 bucket
- Displaying the data schema
- Showing the first few rows of data
- Filtering data for patients with diabetes
- Calculating the average measurement value for diabetic patients