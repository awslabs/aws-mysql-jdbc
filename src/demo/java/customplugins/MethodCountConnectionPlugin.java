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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This connection plugin counts the total number of executed JDBC methods throughout the
 * lifespan of the current connection.
 *
 * <p>All connection plugins must implement the {@link IConnectionPlugin} interface. Since
 * all the connection plugins are chained together, the prior connection plugin needs to
 * invoke the next plugin.
 * Once registered, every connection will create an instance of this connection plugin.
 */
public class MethodCountConnectionPlugin implements IConnectionPlugin {
  private final IConnectionPlugin nextPlugin;
  private final Log logger;
  private final Map<String, Integer> methodCount = new HashMap<>();

  public MethodCountConnectionPlugin(IConnectionPlugin nextPlugin, Log logger) {
    this.nextPlugin = nextPlugin;
    this.logger = logger;
  }

  /**
   * All method calls related to the connection object will be passed to this method as
   * {@code Callable<?> executeJdbcMethod}.
   * This includes methods that may be called frequently, such as:
   * <ul>
   *   <li>{@link ResultSet#next()}</li>
   *   <li>{@link ResultSet#getString(int)}</li>
   * </ul>
   */
  @Override
  public Object execute(
      Class<?> methodInvokeOn,
      String methodName,
      Callable<?> executeJdbcMethod, Object[] args) throws Exception {
    // Increment the number of calls to this method.
    methodCount.merge(methodName, 1, Integer::sum);
    // Traverse the connection plugin chain by invoking the `execute` method in the
    // next plugin.
    return this.nextPlugin.execute(methodInvokeOn, methodName, executeJdbcMethod, args);
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

  /**
   * This method is called when the connection closes.
   * If this connection plugin has any background threads this is the time to clean up
   * these dangling resources. However, you can also perform other steps you wish before
   * closing the plugin. This sample outputs all the aggregated information during this
   * step.
   */
  @Override
  public void releaseResources() {
    // Output the aggregated information from all methods called throughout the lifespan
    // of the current connection.

    final String leftAlignFormat = "| %-19s | %-10d |\n";
    final StringBuilder logMessage = new StringBuilder();

    logMessage
        .append("** MethodCountConnectionPlugin Summary **\n")
        .append("+---------------------+------------+\n")
        .append("| Method Executed     | Frequency  |\n")
        .append("+---------------------+------------+\n");

    methodCount.forEach((key, val) -> logMessage.append(String.format(
        leftAlignFormat,
        key,
        val)));
    logMessage.append("+---------------------+------------+\n");
    logger.logInfo(logMessage);

    methodCount.clear();

    // Traverse the connection plugin chain by calling the next plugin. This step allows
    // all connection plugins a chance to clean up any dangling resources or perform any
    // last tasks before shutting down.
    // In this sample, `ExecutionTimeConnectionPlugin#releaseResources()` will be called
    // to print out the total execution time of each method.
    this.nextPlugin.releaseResources();
  }
}
