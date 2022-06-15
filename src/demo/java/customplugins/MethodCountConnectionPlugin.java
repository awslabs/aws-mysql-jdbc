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
