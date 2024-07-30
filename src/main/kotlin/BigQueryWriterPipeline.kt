import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.joda.time.Duration
import kotlin.random.Random

class BigQueryWriterPipeline {
    fun run() {
        val options = PipelineOptionsFactory
            .`as`(BigQueryWriterPipelineOptions::class.java)
        val pipeline = Pipeline.create(options)

        // Change to suit your environment
        val projectId = "your-project-id"
        val bucketName = "your-bucket-name"
        val region = "your-region"
        val subnetworkName = "your-subnetwork-name"
        val network = "your-network"
        val subscriptionName = "your-subscription-name"

        options.isStreaming = true
        options.isEnableStreamingEngine = true
        options.runner = DataflowRunner::class.java
        options.project = projectId
        options.region = region
        options.maxNumWorkers = 5
        options.numWorkers = 1
        options.workerMachineType = "n1-standard-2"
        options.autoscalingAlgorithm = DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
        options.experiments = listOf("enable_streaming_engine", "enable_windmill_service")
        options.jobName = "bigquery-writer-pipeline-example"
        options.network = network
        options.subnetwork = "regions/$region/subnetworks/$subnetworkName"
        options.stagingLocation = "gs://$bucketName/staging"
        options.tempLocation = "gs://$bucketName/tmp"
        options.subscriptionName = "projects/${projectId}/subscriptions/$subscriptionName"

        val allDatasetTag = object : TupleTag<KV<String, TableRow>>() {}
        val individualDatasetTag = object : TupleTag<KV<String, TableRow>>() {}

        // Get and decode message from subscription
        val tableRowResult = pipeline
            .apply(
                "ReadMessageFromTopic",
                PubsubIO.readMessagesWithAttributes().fromSubscription(options.subscriptionName)
            )
            .apply(
                "CreateTableRows",
                ParDo.of(object : DoFn<PubsubMessage, KV<String, TableRow>>() {
                    @ProcessElement
                    fun processElement(ctx: ProcessContext) {
                        val dataAll: MutableMap<String, Any> = mutableMapOf(
                            "id" to 1,
                            "name" to "Long",
                            "extra_field" to "test"
                        )
                        val dataIndividual: MutableMap<String, Any> = mutableMapOf(
                            "id" to 1,
                            "name" to "Long"
                        )

                        val tableRowAll = TableRow()
                        tableRowAll.unknownKeys = dataAll

                        val tableRowIndividual = TableRow()
                        tableRowIndividual.unknownKeys = dataIndividual

                        // Random key to randomly write data to 1 of the Individual dataset
                        val randomKey = Random.nextInt(1, 11).toString()

                        // ALL dataset
                        ctx.output(allDatasetTag, KV.of(randomKey, tableRowAll))

                        // Individual dataset
                        ctx.output(individualDatasetTag, KV.of(randomKey, tableRowIndividual))
                    }
                }).withOutputTags(allDatasetTag, TupleTagList.of(individualDatasetTag))
            )

        tableRowResult.get(allDatasetTag)
            .apply(
                "ALL",
                BigQueryIO.write<KV<String, TableRow>>()
                    .to(object : DynamicDestinations<KV<String, TableRow>, String>() {
                        override fun getDestination(element: ValueInSingleWindow<KV<String, TableRow>>?): String {
                            return element!!.value.key
                        }

                        override fun getTable(destination: String?): TableDestination {
                            val tableReference = TableReference()
                                .setProjectId(projectId)
                                .setDatasetId("ALL_TEST")
                                .setTableId("gps_test")
                            return TableDestination(tableReference, null)
                        }

                        override fun getSchema(destination: String): TableSchema? {
                            // return schema with extra field
                            return TableSchema().setFields(
                                mutableListOf(
                                    TableFieldSchema().setName("id").setType("INTEGER"),
                                    TableFieldSchema().setName("name").setType("STRING"),
                                    TableFieldSchema().setName("extra_field").setType("STRING")
                                )
                            )
                        }
                    })
                    .withFormatFunction { it.value } // Get TableRow
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                    .withTriggeringFrequency(Duration.standardMinutes(5))
                    .withAutoSharding()
                    .ignoreUnknownValues()
            )

        tableRowResult.get(individualDatasetTag)
            .apply(
                "Individual",
                BigQueryIO.write<KV<String, TableRow>>()
                    .to(object : DynamicDestinations<KV<String, TableRow>, String>() {
                        override fun getDestination(element: ValueInSingleWindow<KV<String, TableRow>>?): String {
                            return element!!.value.key
                        }

                        override fun getTable(destination: String?): TableDestination {
                            val tableReference = TableReference()
                                .setProjectId(projectId)
                                .setDatasetId("INDIVIDUAL_TEST_$destination")
                                .setTableId("gps_test")
                            return TableDestination(tableReference, null)
                        }

                        override fun getSchema(destination: String): TableSchema? {
                            // return schema with extra field
                            return TableSchema().setFields(
                                mutableListOf(
                                    TableFieldSchema().setName("id").setType("INTEGER"),
                                    TableFieldSchema().setName("name").setType("STRING"),
                                )
                            )
                        }
                    })
                    .withFormatFunction { it.value } // Get TableRow
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                    .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                    .withTriggeringFrequency(Duration.standardMinutes(5))
                    .withAutoSharding()
                    .ignoreUnknownValues()
            )
        pipeline.run()
    }

    interface BigQueryWriterPipelineOptions : PipelineOptions, DataflowPipelineOptions, DataflowPipelineWorkerPoolOptions, GcpOptions {
        @get:Description("Pubsub subscription names")
        @get:Validation.Required
        var subscriptionName: String
    }
}