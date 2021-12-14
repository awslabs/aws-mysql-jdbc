/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package software.aws.rds.jdbc.mysql;

import com.mysql.cj.jdbc.NonRegisteringDriver;
import com.mysql.cj.jdbc.ha.ca.plugins.ConnectionPluginManager;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver should supply a class that implements the Driver interface
 *
 * <p>
 * The DriverManager will try to load as many drivers as it can find and then for any given connection request, it will ask each driver in turn to try to
 * connect to the target URL.
 *
 * <p>
 * It is strongly recommended that each Driver class should be small and standalone so that the Driver class can be loaded and queried without bringing in vast
 * quantities of supporting code.
 *
 * <p>
 * When a Driver class is loaded, it should create an instance of itself and register it with the DriverManager. This means that a user can load and register a
 * driver by doing Class.forName("foo.bah.Driver")
 */
public class Driver extends NonRegisteringDriver {
  //
  // Register ourselves with the DriverManager
  //
  static {
    try {
      DriverManager.registerDriver(new Driver());
      System.out.println("You are using Amazon Web Services (AWS) JDBC Driver for MySQL.");
    } catch (SQLException E) {
      throw new RuntimeException("Can't register driver!");
    }
  }

  /**
   * Set the acceptAwsProtocolOnly property for the driver, which controls whether protocols other than
   * jdbc:postgresql:aws:// will be accepted by the driver. This setting should be set to true when
   * running an application that uses this driver simultaneously with another MySQL
   * driver that supports the same protocols (eg the MySQL JDBC Driver), to ensure the driver
   * protocols do not clash. The property can also be set at the connection level via a connection
   * parameter, which will take priority over this driver-level property.
   *
   * @param awsProtocolOnly enables the acceptAwsProtocolOnly mode of the driver
   */
  public static void setAcceptAwsProtocolOnly(boolean awsProtocolOnly) {
    acceptAwsProtocolOnly = awsProtocolOnly;
  }

  /**
   * Construct a new driver and register it with DriverManager
   *
   * @throws SQLException
   *             if a database error occurs.
   */
  public Driver() throws SQLException {
    // Required for Class.forName().newInstance()
  }

  /**
   * Release all resources currently held up by {@link ConnectionPluginManager}.
   */
  public static void releasePluginManagers() {
    ConnectionPluginManager.releaseAllResources();
  }
}
