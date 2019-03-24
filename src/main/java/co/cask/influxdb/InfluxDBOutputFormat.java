package co.cask.influxdb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OutputFormat to write to InfluxDB database. */
public class InfluxDBOutputFormat extends OutputFormat<NullWritable, Point> {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBOutputFormat.class);

  // Define configuration options and names.
  private static final String URL = "url";

  // Creates a Hadoop Configuration from the InfluxDB Config.
  public static void configure(Configuration configuration, InfluxDBSink.Conf sinkConf) {
    // Map Sink config to hadoop configs.
    configuration.set(URL, sinkConf.getUrl());
  }

  @Override
  public RecordWriter<NullWritable, Point> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    Configuration config = taskAttemptContext.getConfiguration();

    // Build batch settings.

    // Build retry settings.

    // Init the InfluxDB Client.
    InfluxDB influxDB = InfluxDBFactory.connect(config.get(URL), "root", "root");
    influxDB.setDatabase("aTimeSeries");

    LOG.warn("Creating InfluxDB client.");

    // Create the RecordWriter by passing in the Client.
    return new InfluxDBRecordWriter(influxDB, BatchOptions.DEFAULTS, 0);
  }

  /** */
  public class InfluxDBRecordWriter extends RecordWriter<NullWritable, Point> {
    private final InfluxDB influxDB;
    private final AtomicLong failures;
    private final long errorThreshold;
    private AtomicReference<Throwable> error;

    public InfluxDBRecordWriter(InfluxDB influxDB, BatchOptions batchOptions, long errorThreshold)
        throws IOException {
      this.influxDB = influxDB;
      this.error = new AtomicReference<>();
      this.failures = new AtomicLong(0);
      this.errorThreshold = errorThreshold;
      this.influxDB.enableBatch(
          batchOptions.exceptionHandler(
              (points, throwable) -> {
                error.set(throwable);
                long counter = 0;
                for (Object i : points) {
                  counter++;
                }
                failures.addAndGet(counter);
              }));
    }

    private void handleErrorIfAny() throws IOException {
      if (failures.get() > errorThreshold) {
        throw new IOException(
            String.format("Failed to publish %s records", failures.get()), error.get());
      }
    }

    @Override
    public void write(NullWritable key, Point point) throws IOException {
      handleErrorIfAny();
      influxDB.write(point);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      try {
        // flush batches and wait until done.
        if (influxDB.isBatchEnabled()) {
          influxDB.flush();
        }
        handleErrorIfAny();
      } catch (Exception e) {
        throw new IOException("Error publishing records to InfluxDB", e);
      } finally {
        try {
          // close client
          influxDB.close();
        } catch (Exception e) {
          // if there is an exception while shutting down, we only log
          LOG.debug("Exception while shutting down InfluxDBClient ", e);
        }
      }
    }
  }

  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    // do nothing.
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    return new OutputCommitter() {

      @Override
      public void setupJob(JobContext jobContext) {
        // do nothing for now.
      }

      @Override
      public void setupTask(TaskAttemptContext taskAttemptContext) {
        // do nothing for now.
      }

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskAttemptContext) {
        // do nothing for now.
      }

      @Override
      public void abortTask(TaskAttemptContext taskAttemptContext) {
        // do nothing for now.
      }
    };
  }
}
