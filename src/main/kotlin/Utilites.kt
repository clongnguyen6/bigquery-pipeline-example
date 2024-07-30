import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.BigQueryException
import com.google.cloud.bigquery.Dataset
import com.google.cloud.bigquery.DatasetInfo
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.LegacySQLTypeName
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo

fun createDataset(datasetName: String) {
    try {
        val bigquery = BigQueryOptions.getDefaultInstance().service
        val datasetInfo = DatasetInfo.newBuilder(datasetName).build()
        val newDataset: Dataset = bigquery.create(datasetInfo)
        val newDatasetName: String = newDataset.datasetId.dataset
        println("$newDatasetName created successfully")
    } catch (e: BigQueryException) {
        println("Dataset was not created. \n$e")
    }
}

fun createTable(datasetName: String, tableName: String, schema: Schema) {
    try {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        val bigquery = BigQueryOptions.getDefaultInstance().service
        val tableId = TableId.of(datasetName, tableName)
        val tableDefinition: TableDefinition = StandardTableDefinition.of(schema)
        val tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build()
        bigquery.create(tableInfo)
        println("Table created successfully")
    } catch (e: BigQueryException) {
        println("Table was not created. \n$e")
    }
}

fun main() {
    // Create ALL_TEST dataset
    createDataset("ALL_TEST")

    // Create Individual datasets
    for (i in 0 until 11) {
        createDataset("INDIVIDUAL_TEST_${i}")
    }
    val fieldsALL = listOf(
        Field.of("id", LegacySQLTypeName.INTEGER),
        Field.of("name", LegacySQLTypeName.STRING),
        Field.of("extra_field", LegacySQLTypeName.STRING),
        )
    val schemaALL = Schema.of(fieldsALL)

    val fieldsIndividual = listOf(
        Field.of("id", LegacySQLTypeName.INTEGER),
        Field.of("name", LegacySQLTypeName.STRING),
    )
    val schemaIndividual = Schema.of(fieldsIndividual)

    // Create table gps_test in ALL_TEST dataset:
    createTable("ALL_TEST", "gps_test", schemaALL)

    // Create table gps_test in INDIVIDUAL datasets:
    for (i in 0 until 11) {
        createTable("INDIVIDUAL_TEST_${i}", "gps_test", schemaIndividual)
    }
}