/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMysqlPerformanceIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  private static final int REPEAT_TIMES = 5;
  private static final List<PerfStat> enhancedFailureMonitoringPerfDataList = new ArrayList<>();

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    AuroraMysqlIntegrationBaseTest.setUp();
  }

  @AfterAll
  public static void cleanUp() throws IOException {
    doWritePerfDataToFile("./build/reports/tests/FailureDetectionResults_EnhancedMonitoring.xlsx", enhancedFailureMonitoringPerfDataList);
  }

  private static void doWritePerfDataToFile(String fileName, List<PerfStat> dataList) throws IOException {
    try (XSSFWorkbook workbook = new XSSFWorkbook()) {
      // Title
      final XSSFSheet sheet = workbook.createSheet("FailureDetectionResults");

      // Header
      Row row = sheet.createRow(0);
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

      // Data
      for (int rows = 0; rows < dataList.size(); rows++) {
        PerfStat perfStat = dataList.get(rows);
        row = sheet.createRow(rows + 1);

        cell = row.createCell(0);
        cell.setCellValue(perfStat.paramDetectionTime);
        cell = row.createCell(1);
        cell.setCellValue(perfStat.paramDetectionInterval);
        cell = row.createCell(2);
        cell.setCellValue(perfStat.paramDetectionCount);
        cell = row.createCell(3);
        cell.setCellValue(perfStat.paramNetworkOutageDelayMillis);
        cell = row.createCell(4);
        cell.setCellValue(perfStat.minFailureDetectionTimeMillis);
        cell = row.createCell(5);
        cell.setCellValue(perfStat.maxFailureDetectionTimeMillis);
        cell = row.createCell(6);
        cell.setCellValue(perfStat.avgFailureDetectionTimeMillis);
      }

      // Write to file
      final File newExcelFile = new File(fileName);
      newExcelFile.createNewFile();
      try (FileOutputStream fileOut = new FileOutputStream(newExcelFile)) {
        workbook.write(fileOut);
      }
    }
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

    PerfStat data = doMeasurePerformance(detectionTime, detectionInterval, detectionCount, sleepDelayMillis, REPEAT_TIMES, props);
    enhancedFailureMonitoringPerfDataList.add(data);
  }

  private PerfStat doMeasurePerformance(
          int detectionTime, int detectionInterval, int detectionCount,
          int sleepDelayMillis, int repeatTimes, Properties props)
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

    return new PerfStat(detectionTime, detectionInterval, detectionCount, sleepDelayMillis, min, max, avg);
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

  private class PerfStat {
    public PerfStat(int paramDetectionTime, int paramDetectionInterval, int paramDetectionCount,
                    int paramNetworkOutageDelayMillis, int minFailureDetectionTimeMillis,
                    int maxFailureDetectionTimeMillis, int avgFailureDetectionTimeMillis) {
      this.paramDetectionTime = paramDetectionTime;
      this.paramDetectionInterval = paramDetectionInterval;
      this.paramDetectionCount = paramDetectionCount;
      this.paramNetworkOutageDelayMillis = paramNetworkOutageDelayMillis;
      this.minFailureDetectionTimeMillis = minFailureDetectionTimeMillis;
      this.maxFailureDetectionTimeMillis = maxFailureDetectionTimeMillis;
      this.avgFailureDetectionTimeMillis = avgFailureDetectionTimeMillis;
    }

    public int paramDetectionTime;
    public int paramDetectionInterval;
    public int paramDetectionCount;
    public int paramNetworkOutageDelayMillis;
    public int minFailureDetectionTimeMillis;
    public int maxFailureDetectionTimeMillis;
    public int avgFailureDetectionTimeMillis;
  }

}
