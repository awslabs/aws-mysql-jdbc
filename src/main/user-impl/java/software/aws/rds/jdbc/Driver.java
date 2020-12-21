package software.aws.rds.jdbc;

import com.mysql.cj.jdbc.NonRegisteringDriver;

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
public class Driver extends NonRegisteringDriver implements java.sql.Driver {
  //
  // Register ourselves with the DriverManager
  //
  static {
    try {
      java.sql.DriverManager.registerDriver(new software.aws.rds.jdbc.Driver());
    } catch (SQLException E) {
      throw new RuntimeException("Can't register driver!");
    }
  }

  static {
    System.out.println("You are using Atlas Driver, you can also use 'software.aws.rds.jdbc.Driver()' to register");
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
