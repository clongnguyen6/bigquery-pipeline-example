# bigquery-pipeline-example
This repository contains an example pipeline that demonstrates the INVALID_ARGUMENT exception issue when streaming data to BigQuery. The project uses Gradle and Kotlin.


# Pipeline Setup and Running Guide

## Requirements

1. **Service Account**
    - A service account with the following permissions is required:
        - BigQuery Admin
        - Dataflow Worker
        - Storage Object Admin

2. **Environment Variables**
    - Export the service account key to an environment variable, [export here](https://github.com/clongnguyen6/bigquery-pipeline-example/blob/main/build.gradle.kts#L40-L43):
      ```sh
      tasks {
         "run"(JavaExec::class) {
            environment("GOOGLE_APPLICATION_CREDENTIALS", "/service-account.json")
         }
      }
      ```

3. **Pub/Sub**
    - Create a topic and a subscription in Pub/Sub.

4. **Network and Subnetwork**
    - Create a network and subnetwork.

5. **BigQuery Datasets and Tables** ([you can refer to this script](https://github.com/clongnguyen6/bigquery-pipeline-example/blob/main/src/main/kotlin/Utilites.kt#L40))
    - Create a dataset named `ALL_TEST`. In this dataset, create a table named `gps_test` with the following schema:
        - `id: INTEGER`
        - `name: STRING`
        - `extra_field: STRING`

    - Create the following datasets: `INDIVIDUAL_TEST_1`, `INDIVIDUAL_TEST_2`, `INDIVIDUAL_TEST_3`, `INDIVIDUAL_TEST_4`, `INDIVIDUAL_TEST_5`, `INDIVIDUAL_TEST_6`, `INDIVIDUAL_TEST_7`, `INDIVIDUAL_TEST_8`, `INDIVIDUAL_TEST_9`, `INDIVIDUAL_TEST_10`. In each of these datasets, create a table named `gps_test` with the following schema:
        - `id: INTEGER`
        - `name: STRING`

## Configuration

- Update the values in the code to match your environment, [here](https://github.com/clongnguyen6/bigquery-pipeline-example/blob/main/src/main/kotlin/BigQueryWriterPipeline.kt#L35-L41):
   ```kotlin
     // Change to suit your environment
     val projectId = "your-project-id"
     val bucketName = "your-bucket-name"
     val region = "your-region"
     val subnetworkName = "your-subnetwork-name"
     val network = "your-network"
     val subscriptionName = "your-subscription-name"
   ```

## Run Pipeline

- Use Gradle to run the pipeline:
  ```sh
  gradle run
  ```

- Check the Job in the Dataflow Job service.
- Publish messages to the topic for the job to process and see the results. (According to the results running in our environment, the error will appear after 15 minutes, with ~7k messages, throughput ~7 elements/s)
