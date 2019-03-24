package co.cask.influxdb;

import java.util.concurrent.TimeUnit;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.junit.Test;

public class InfluxDBTest {

  @Test
  public void testBasic() throws Throwable {
    InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");

    String dbName = "aTimeSeries";
    influxDB.query(new Query("CREATE DATABASE " + dbName));
    influxDB.setDatabase(dbName);

    influxDB.enableBatch(BatchOptions.DEFAULTS);

    Point point =
        Point.measurement("cpu")
            .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
            .addField("idle", 90L)
            .addField("user", 9L)
            .addField("system", 2L)
            .build();

    influxDB.write(point);

    influxDB.close();
  }
}
