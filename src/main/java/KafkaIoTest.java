import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;



public class KafkaIoTest {


	public static interface MyOptions extends DataflowPipelineOptions {

		@Description("Kafka Bootstrap Servers")
        @Default.String("10.128.0.2:9092")
        String getBootstrap();
        void setBootstrap(String s);

		@Description("Kafka Topic Name")
        @Default.String("test")
        String getInputTopic();
        void setInputTopic(String s);

		@Description("Output BigQuery table <project_id>:<dataset_id>.<table_id>")
		@Default.String("louisa-dataflow-demo:demos.streamdemo")
		String getOutput();
		void setOutput(String s);

	}


	@SuppressWarnings("serial")
	public static void main(String[] args) {
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		options.setStreaming(true);
		Pipeline p = Pipeline.create(options);

		String topic = options.getInputTopic();
		String output = options.getOutput();

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("timestamp").setType("TIMESTAMP"));
		fields.add(new TableFieldSchema().setName("kafkamessage").setType("STRING"));
		TableSchema schema = new TableSchema().setFields(fields);

		p 

				.apply(KafkaIO.<Long, String>read()
			        .withBootstrapServers(options.getBootstrap())
			        .withTopic(options.getInputTopic())
			        .withKeyDeserializer(LongDeserializer.class)
			        .withValueDeserializer(StringDeserializer.class))

			    // .apply(Values.<String>create()) // PCollection<String>

            	// .apply("window",
            	// 	Window.<KafkaRecord<Long, String>>into(FixedWindows
            	// 		.of(Duration.standardMinutes(1))))


				.apply("window",
						Window.into(SlidingWindows
								.of(Duration.standardMinutes(2))
								.every(Duration.standardSeconds(30)))) 

			    .apply("ExtractWords",ParDo.of(new DoFn<KafkaRecord<Long, String>, String>() {
		            @ProcessElement
		            public void processElement(ProcessContext c) throws Exception{
		                KafkaRecord<Long, String> record = c.element();
		                c.output(record.getKV().getValue());
		            }
		        }))


				.apply("ToBQRow", ParDo.of(new DoFn<String, TableRow>() {
					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						TableRow row = new TableRow();
						row.set("timestamp", Instant.now().toString());
						row.set("kafkamessage", c.element());
						c.output(row);
					}
				}))

				.apply(BigQueryIO.writeTableRows().to(output)
						.withSchema(schema)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

		p.run();
	}
}
