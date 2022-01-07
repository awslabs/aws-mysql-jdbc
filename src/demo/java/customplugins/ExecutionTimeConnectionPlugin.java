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

package customplugins;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.ha.plugins.IConnectionPlugin;
import com.mysql.cj.log.Log;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This connection plugin tracks the execution time of all the given JDBC method throughout
 * the lifespan of the current connection.
 *
 * <p>During the cleanup phase when {@link ExecutionTimeConnectionPlugin#releaseResources()}
 * is called, this plugin logs all the methods executed and time spent on each execution
 * in milliseconds.
 */
public class ExecutionTimeConnectionPlugin implements IConnectionPlugin {
  final long initializeTime;
  final IConnectionPlugin nextPlugin;
  private final Log logger;
  private final Map<String, Long> methodExecutionTimes = new HashMap<>();

  public ExecutionTimeConnectionPlugin(
      IConnectionPlugin nextPlugin,
      Log logger) {
    this.nextPlugin = nextPlugin;
    this.logger = logger;

    initializeTime = System.currentTimeMillis();
  }

  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeJdbcMethod, Object[] args)
      throws Exception {
    // This `execute` measures the time it takes for the remaining connection plugins to
    // execute the given method call.
    final long startTime = System.nanoTime();
    final Object executeResult =
        this.nextPlugin.execute(methodInvokeOn, methodName, executeJdbcMethod, args);
    final long elapsedTime = System.nanoTime() - startTime;
    methodExecutionTimes.merge(
        methodName,
        elapsedTime / 1000000,
        Long::sum);

    return executeResult;
  }

  @Override
  public void transactionBegun() {
    this.nextPlugin.transactionBegun();
  }

  @Override
  public void transactionCompleted() {
    this.nextPlugin.transactionCompleted();
  }

  @Override
  public void openInitialConnection(ConnectionUrl connectionUrl) throws SQLException {
    this.nextPlugin.openInitialConnection(connectionUrl);
  }

  @Override
  public void releaseResources() {
    // Output the aggregated information from all methods called throughout the lifespan
    // of the current connection.
    final long connectionUptime = System.nanoTime() - initializeTime;
    final String leftAlignFormat = "| %-19s | %-10s |\n";
    final StringBuilder logMessage = new StringBuilder();

    logMessage.append("** ExecutionTimeConnectionPlugin Summary **\n");
    logMessage.append(String.format(
        "Connection Uptime: %ds\n",
        connectionUptime / 1000000
    ));

    logMessage
        .append("** Method Execution Time **\n")
        .append("+---------------------+------------+\n")
        .append("| Method Executed     | Total Time |\n")
        .append("+---------------------+------------+\n");

    methodExecutionTimes.forEach((key, val) -> logMessage.append(String.format(
        leftAlignFormat,
        key,
        val + "ms")));
    logMessage.append("+---------------------+------------+\n");
    logger.logInfo(logMessage);

    methodExecutionTimes.clear();

    // Traverse the connection plugin chain by calling the next plugin. This step allows
    // all connection plugins a chance to clean up any dangling resources or perform any
    // last tasks before shutting down.
    this.nextPlugin.releaseResources();
  }
}
