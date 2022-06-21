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
