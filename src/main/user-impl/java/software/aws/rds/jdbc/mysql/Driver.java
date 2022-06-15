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

package software.aws.rds.jdbc.mysql;

import com.mysql.cj.jdbc.NonRegisteringDriver;

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
}
