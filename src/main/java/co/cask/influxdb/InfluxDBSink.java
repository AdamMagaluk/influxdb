/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.influxdb;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.LineageRecorder;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch Sink that writes to a InfluxDB. Each record will be written metric entry in InfluxDB.
 *
 * <p>StructuredRecord is the first parameter because that is the input to the sink. The second and
 * third parameters are the key and value expected by Hadoop.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(InfluxDBSink.NAME)
@Description("Writes to a InfluxDB database.")
public class InfluxDBSink extends BatchSink<StructuredRecord, NullWritable, Point> {
  public static final String NAME = "InfluxDB";
  private final Conf config;

  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBOutputFormat.class);

  // CDAP will pass in a config with its fields populated based on the configuration given when
  // creating the pipeline.
  public InfluxDBSink(Conf config) {
    this.config = config;
  }

  // configurePipeline is called exactly once when the pipeline is being created.
  // Any static configuration should be performed here.
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
  }

  // prepareRun is called before every pipeline run, and is used to configure what the input should
  // be, as well as any arguments the input should use. It is called by the client that is
  // submitting
  // the batch job.
  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    // validate config.
    config.validate();

    // Create a Hadoop Configuration from sink config.
    Configuration configuration = new Configuration();
    InfluxDBOutputFormat.configure(configuration, config);

    // TODO: Add reference name to the Configuration and use instead of constant.
    String referenceName = "influxdb";

    // Setup lineage.
    Schema inputSchema = context.getInputSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, referenceName);
    lineageRecorder.createExternalDataset(inputSchema);

    // Setup output.
    context.addOutput(
        Output.of(
            referenceName,
            new SinkOutputFormatProvider(InfluxDBOutputFormat.class, configuration)));

    // record field level lineage information
    if (inputSchema != null
        && inputSchema.getFields() != null
        && !inputSchema.getFields().isEmpty()) {
      lineageRecorder.recordWrite(
          "Write",
          "Wrote to InfluxDB",
          inputSchema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  // onRunFinish is called at the end of the pipeline run by the client that submitted the batch
  // job.
  @Override
  public void onRunFinish(boolean succeeded, BatchSinkContext context) {
    // perform any actions that should happen at the end of the run.
  }

  // initialize is called by each job executor before any call to transform is made.
  // This occurs at the start of the batch job run, after the job has been successfully submitted.
  // For example, if mapreduce is the execution engine, each mapper will call initialize at the
  // start of the program.
  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    // create any resources required by transform()
  }

  // destroy is called by each job executor at the end of its life.
  // For example, if mapreduce is the execution engine, each mapper will call destroy at the end of
  // the program.
  @Override
  public void destroy() {
    // clean up any resources created by initialize
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Point>> emitter)
      throws Exception {
    // Hardcode a Point for now.
    Point point =
        Point.measurement("cpu")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("idle", 20L)
            .addField("user", 12L)
            .addField("system", 2L)
            .build();

    emitter.emit(new KeyValue<>(NullWritable.get(), point));
  }

  /** Config properties for the plugin. */
  public static class Conf extends PluginConfig {
    public static final String URL = "url";

    // The name annotation tells CDAP what the property name is. It is optional, and defaults to the
    // variable name.
    // Note: only primitives (including boxed types) and string are the types that are supported
    // Macro enabled properties can be set to a placeholder value ${key} when the pipeline is
    // deployed.
    // At runtime, the value for 'key' can be given and substituted in.
    @Macro
    @Name(URL)
    @Description("The URL of the InfluxDB server to write to.")
    private String url;

    // Use a no-args constructor to set field defaults.
    public Conf() {
      url = "";
    }

    // Validate config.
    public void validate() {

      // Parse the URI
      try {
        new URI(url);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Unable to parse url: " + url, e);
      }
    }

    public String getUrl() {
      return url;
    }
  }
}
