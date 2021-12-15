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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * This is a simple application for creating and using custom connection plugins.
 */
public class SampleApplication {
  private static final String PREFIX = "jdbc:mysql:aws://";
  private static final String CONNECTION_STRING = PREFIX + System.getenv("host");
  private static final String USER = System.getenv("user");
  private static final String PASSWORD = System.getenv("password");
  private static final String QUERY = System.getenv("query");

  public static void main(String[] args) throws ClassNotFoundException, SQLException {
    Class.forName("software.aws.rds.jdbc.mysql.Driver");
    final Properties properties = new Properties();
    properties.setProperty("user", USER);
    properties.setProperty("password", PASSWORD);
    properties.setProperty("logger", "StandardLogger");

    final String methodCountConnectionPluginFactoryClassName = MethodCountConnectionPluginFactory.class.getName();
    final String executionMeasurementPluginFactoryClassName =
        ExecutionTimeConnectionPluginFactory.class.getName();

    // To use custom connection plugins, set the connectionPluginFactories to a
    // comma-separated string containing the fully-qualified class names of custom plugin
    // factories to use.
    properties.setProperty(
        "connectionPluginFactories",
        String.format("%s,%s",
            methodCountConnectionPluginFactoryClassName,
            executionMeasurementPluginFactoryClassName));

    try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
      try (Statement statement = conn.createStatement()) {
        try (ResultSet result = statement.executeQuery(QUERY)) {
          final int cols = result.getMetaData().getColumnCount();
          while (result.next()) {
            for (int i = 1; i < cols; i++) {
              System.out.println(result.getString(i));
            }
          }
        }
      }
    }
  }
}
