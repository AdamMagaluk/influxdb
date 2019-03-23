package co.cask.influxdb;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** OutputFormat to write to InfluxDB database. */
public class InfluxDBOutputFormat extends OutputFormat<NullWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBOutputFormat.class);

  // Define configuration options and names.

  // Creates a Hadoop Configuration from the InfluxDB Config.
  public static void configure(Configuration configuration, InfluxDBSink.Conf sinkConf) {
    // Map Sink config to hadoop configs.

  }

  @Override
  public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException {
    // Build batch settings.

    // Build retry settings.

    // Init the InfluxDB Client.

    LOG.warn("Creating InfluxDB client.");

    // Create the RecordWriter by passing in the Client.
    return new InfluxDBRecordWriter();
  }

  /** */
  public class InfluxDBRecordWriter extends RecordWriter<NullWritable, Text> {
    private final AtomicLong failures;
    private final AtomicReference<Throwable> error;

    public InfluxDBRecordWriter() throws IOException {
      this.error = new AtomicReference<>();
      this.failures = new AtomicLong(0);
    }

    @Override
    public void write(NullWritable key, Text value) throws IOException {
      LOG.warn("Writing entry to influxdb ", value);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
      try {
        // flush batches and wait until done.
        LOG.warn("Shutting down InfluxDB client.");

      } catch (Exception e) {
        throw new IOException("Error publishing records to InfluxDB", e);
      } finally {
        try {
          // close client
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
