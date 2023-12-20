/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0
 * (GPLv2), as published by the Free Software Foundation, with the
 * following additional permissions:
 *
 * This program is distributed with certain software that is licensed
 * under separate terms, as designated in a particular file or component
 * or in the license documentation. Without limiting your rights under
 * the GPLv2, the authors of this program hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with the program.
 *
 * Without limiting the foregoing grant of rights under the GPLv2 and
 * additional permission as to separately licensed software, this
 * program is also subject to the Universal FOSS Exception, version 1.0,
 * a copy of which can be found along with its FAQ at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html.
 */

package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.ha.plugins.failover.FailoverConnectionPluginFactory;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMysqlPerformanceIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  protected static final int REPEAT_TIMES = 5;
  protected static final int FAILOVER_TIMEOUT_MS = 40000;
  protected static final List<PerfStatMonitoring> enhancedFailureMonitoringPerfDataList = new ArrayList<>();
  protected static final List<PerfStatMonitoring> failoverWithEfmPerfDataList = new ArrayList<>();
  protected static final List<PerfStatSocketTimeout> failoverWithSocketTimeoutPerfDataList = new ArrayList<>();

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    AuroraMysqlIntegrationBaseTest.setUp();
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile("./build/reports/tests/EnhancedMonitoring.xlsx", enhancedFailureMonitoringPerfDataList);
    doWritePerfDataToFile("./build/reports/tests/FailoverWithEnhancedMonitoring.xlsx", failoverWithEfmPerfDataList);
    doWritePerfDataToFile("./build/reports/tests/FailoverWithSocketTimeout.xlsx", failoverWithSocketTimeoutPerfDataList);
  }

  protected static void doWritePerfDataToFile(String fileName, List<? extends PerfStatBase> dataList) throws IOException {
    if (dataList.isEmpty()) {
      return;
    }

    try (XSSFWorkbook workbook = new XSSFWorkbook()) {

      final XSSFSheet sheet = workbook.createSheet("PerformanceResults");

      for (int rows = 0; rows < dataList.size(); rows++) {
        PerfStatBase perfStat = dataList.get(rows);
        Row row;

        if (rows == 0) {
          // Header
          row = sheet.createRow(0);
          perfStat.writeHeader(row);
        }

        row = sheet.createRow(rows + 1);
        perfStat.writeData(row);
      }

      // Write to file
      final File newExcelFile = new File(fileName);
      newExcelFile.createNewFile();
      try (FileOutputStream fileOut = new FileOutputStream(newExcelFile)) {
        workbook.write(fileOut);
      }
    }
    dataList.clear();
  }

  @ParameterizedTest
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailureDetectionTime_EnhancedMonitoring(int detectionTime, int detectionInterval, int detectionCount, int sleepDelayMillis)
    throws SQLException {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyKey.failureDetectionTime.getKeyName(), Integer.toString(detectionTime));
    props.setProperty(PropertyKey.failureDetectionInterval.getKeyName(), Integer.toString(detectionInterval));
    props.setProperty(PropertyKey.failureDetectionCount.getKeyName(), Integer.toString(detectionCount));
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), Boolean.FALSE.toString());

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, false, data);
    data.paramDetectionTime = detectionTime;
    data.paramDetectionInterval = detectionInterval;
    data.paramDetectionCount = detectionCount;
    enhancedFailureMonitoringPerfDataList.add(data);
  }

  @ParameterizedTest
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailoverTime_EnhancedMonitoring(int detectionTime, int detectionInterval, int detectionCount, int sleepDelayMillis)
          throws SQLException {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyKey.failureDetectionTime.getKeyName(), Integer.toString(detectionTime));
    props.setProperty(PropertyKey.failureDetectionInterval.getKeyName(), Integer.toString(detectionInterval));
    props.setProperty(PropertyKey.failureDetectionCount.getKeyName(), Integer.toString(detectionCount));
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), Integer.toString(40000));

    final PerfStatMonitoring data = new PerfStatMonitoring();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, true, data);
    data.paramDetectionTime = detectionTime;
    data.paramDetectionInterval = detectionInterval;
    data.paramDetectionCount = detectionCount;
    failoverWithEfmPerfDataList.add(data);
  }

  @ParameterizedTest
  @MethodSource("generateFailoverSocketTimeoutTimeParams")
  public void test_FailoverTime_SocketTimeout(int socketTimeout, int sleepDelayMillis)
          throws SQLException {
    final Properties props = initDefaultPropsNoTimeouts();
    // The goal of this performance test is to check how socket timeout changes overall failover time
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), Integer.toString(socketTimeout));
    // Loads just failover plugin; don't load Enhanced Failure Monitoring plugin
    props.setProperty(PropertyKey.connectionPluginFactories.getKeyName(), FailoverConnectionPluginFactory.class.getName());
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), Integer.toString(FAILOVER_TIMEOUT_MS));

    final PerfStatSocketTimeout data = new PerfStatSocketTimeout();
    doMeasurePerformance(sleepDelayMillis, REPEAT_TIMES, props, true, data);
    data.paramSocketTimeout = socketTimeout;
    failoverWithSocketTimeoutPerfDataList.add(data);
  }

  private void doMeasurePerformance(
          int sleepDelayMillis, int repeatTimes, Properties props, boolean openReadOnlyConnection, PerfStatBase data)
    throws SQLException {

    final String QUERY = "select sleep(600)"; // 600s -> 10min
    final AtomicLong downtime = new AtomicLong();
    final List<Integer> elapsedTimes = new ArrayList<>(repeatTimes);

    for (int i = 0; i < repeatTimes; i++) {
      downtime.set(0);

      // Thread to stop network
      final Thread thread = new Thread(() -> {
        try {
          Thread.sleep(sleepDelayMillis);
          // Kill network
          containerHelper.disableConnectivity(proxyInstance_1);
          downtime.set(System.currentTimeMillis());
        } catch (IOException ioException) {
          fail("Toxics were already set, should not happen");
        } catch (InterruptedException interruptedException) {
          // Ignore, stop the thread
        }
      });

      try (final Connection conn = openConnectionWithRetry(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props);
           final Statement statement = conn.createStatement()) {

        conn.setReadOnly(openReadOnlyConnection);
        thread.start();

        // Execute long query
        try (final ResultSet result = statement.executeQuery(QUERY)) {
          fail("Sleep query finished, should not be possible with network downed.");
        } catch (SQLException throwables) { // Catching executing query
          // Calculate and add detection time
          final int failureTime = (int) (System.currentTimeMillis() - downtime.get());
          elapsedTimes.add(failureTime);
        }

      } finally {
        thread.interrupt(); // Ensure thread has stopped running
        containerHelper.enableConnectivity(proxyInstance_1);
      }
    }

    int min = elapsedTimes.stream().min(Integer::compare).orElse(0);
    int max = elapsedTimes.stream().max(Integer::compare).orElse(0);
    int avg = (int)elapsedTimes.stream().mapToInt(a -> a).summaryStatistics().getAverage();

    data.paramNetworkOutageDelayMillis = sleepDelayMillis;
    data.minFailureDetectionTimeMillis = min;
    data.maxFailureDetectionTimeMillis = max;
    data.avgFailureDetectionTimeMillis = avg;
  }

  private Connection openConnectionWithRetry(String url, int port, Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        conn = connectToInstance(url, port, props);
      } catch (SQLException sqlEx) {
        // ignore, try to connect again
      }
      connectCount++;
    }

    if (conn == null) {
      fail("Can't connect to " + url);
    }
    return conn;
  }

  private static Stream<Arguments> generateFailureDetectionTimeParams() {
    // detectionTime, detectionInterval, detectionCount, sleepDelayMS
    return Stream.of(
      // Defaults
      Arguments.of(30000, 5000, 3, 5000),
      Arguments.of(30000, 5000, 3, 10000),
      Arguments.of(30000, 5000, 3, 15000),
      Arguments.of(30000, 5000, 3, 20000),
      Arguments.of(30000, 5000, 3, 25000),
      Arguments.of(30000, 5000, 3, 30000),
      Arguments.of(30000, 5000, 3, 35000),
      Arguments.of(30000, 5000, 3, 40000),
      Arguments.of(30000, 5000, 3, 50000),
      Arguments.of(30000, 5000, 3, 60000),

      // Aggressive detection scheme
      Arguments.of(6000, 1000, 1, 1000),
      Arguments.of(6000, 1000, 1, 2000),
      Arguments.of(6000, 1000, 1, 3000),
      Arguments.of(6000, 1000, 1, 4000),
      Arguments.of(6000, 1000, 1, 5000),
      Arguments.of(6000, 1000, 1, 6000),
      Arguments.of(6000, 1000, 1, 7000),
      Arguments.of(6000, 1000, 1, 8000),
      Arguments.of(6000, 1000, 1, 9000),
      Arguments.of(6000, 1000, 1, 10000));
  }

  private static Stream<Arguments> generateFailoverSocketTimeoutTimeParams() {
    // socketTimeout, sleepDelayMS
    return Stream.of(
      Arguments.of(30000, 5000),
      Arguments.of(30000, 10000),
      Arguments.of(30000, 15000),
      Arguments.of(30000, 25000),
      Arguments.of(30000, 20000),
      Arguments.of(30000, 30000));
  }

  private abstract class PerfStatBase {
    public int paramNetworkOutageDelayMillis;
    public int minFailureDetectionTimeMillis;
    public int maxFailureDetectionTimeMillis;
    public int avgFailureDetectionTimeMillis;

    public abstract void writeHeader(Row row);
    public abstract void writeData(Row row);
  }

  private class PerfStatMonitoring extends PerfStatBase {
    public int paramDetectionTime;
    public int paramDetectionInterval;
    public int paramDetectionCount;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("FailureDetectionGraceTime");
      cell = row.createCell(1);
      cell.setCellValue("FailureDetectionInterval");
      cell = row.createCell(2);
      cell.setCellValue("FailureDetectionCount");
      cell = row.createCell(3);
      cell.setCellValue("NetworkOutageDelayMillis");
      cell = row.createCell(4);
      cell.setCellValue("MinFailureDetectionTime");
      cell = row.createCell(5);
      cell.setCellValue("MaxFailureDetectionTime");
      cell = row.createCell(6);
      cell.setCellValue("AvgFailureDetectionTime");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.paramDetectionTime);
      cell = row.createCell(1);
      cell.setCellValue(this.paramDetectionInterval);
      cell = row.createCell(2);
      cell.setCellValue(this.paramDetectionCount);
      cell = row.createCell(3);
      cell.setCellValue(this.paramNetworkOutageDelayMillis);
      cell = row.createCell(4);
      cell.setCellValue(this.minFailureDetectionTimeMillis);
      cell = row.createCell(5);
      cell.setCellValue(this.maxFailureDetectionTimeMillis);
      cell = row.createCell(6);
      cell.setCellValue(this.avgFailureDetectionTimeMillis);
    }
  }

  private class PerfStatSocketTimeout extends PerfStatBase {
    public int paramSocketTimeout;

    @Override
    public void writeHeader(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue("SocketTimeout");
      cell = row.createCell(1);
      cell.setCellValue("NetworkOutageDelayMillis");
      cell = row.createCell(2);
      cell.setCellValue("MinFailureDetectionTime");
      cell = row.createCell(3);
      cell.setCellValue("MaxFailureDetectionTime");
      cell = row.createCell(4);
      cell.setCellValue("AvgFailureDetectionTime");
    }

    @Override
    public void writeData(Row row) {
      Cell cell = row.createCell(0);
      cell.setCellValue(this.paramSocketTimeout);
      cell = row.createCell(1);
      cell.setCellValue(this.paramNetworkOutageDelayMillis);
      cell = row.createCell(2);
      cell.setCellValue(this.minFailureDetectionTimeMillis);
      cell = row.createCell(3);
      cell.setCellValue(this.maxFailureDetectionTimeMillis);
      cell = row.createCell(4);
      cell.setCellValue(this.avgFailureDetectionTimeMillis);
    }
  }

}
