package testsuite.failover;

//import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;

@Disabled
public class HardFailure {

  public HardFailure() throws SQLException {
    DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
  }

  static final String ATLAS_DB_URL = "jdbc:mysql://seners-fourthcluster.cluster-cmdmxqrbqtbc.us-west-2-orion.rds.amazonaws.com/sbtest";

  static final String USER = "reinvent";
  static final String PASS = "reinvent";

//    static {
//        Security.setProperty("networkaddress.cache.ttl" , "1");
//    }

  private Connection createAtlasConnection() throws SQLException {
    return DriverManager.getConnection(
      ATLAS_DB_URL
      + "?allowMultiQueries=true"
      + "&connectTimeout=30000"
      + "&socketTimeout=30000"
      + "&gatherPerfMetrics=false"
      ,
      USER,
      PASS);
  }

  private String queryInstanceId(Connection connection) throws SQLException {
    final Statement myStmt = connection.createStatement();
    final ResultSet result = myStmt.executeQuery("select @@aurora_server_id");
    if (result.next()) {
      return result.getString("@@aurora_server_id");
    }
    throw new SQLException();
  }

  private Connection createConnection() {
    while (true) {
      try {
        Connection conn = createAtlasConnection();

        conn.setAutoCommit(true);
        return conn;
      } catch (Exception e) {
        System.out.println("Failed to connect.\n" + e);
        try {
          Thread.sleep(1000);
          System.out.println("Retrying...");
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  private static Statement createStatement(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    //stmt.setQueryTimeout(5);
    return stmt;
  }

  @Test
  void testtest() throws SQLException {
    Connection conn = null;
    Statement stmt = null;
    try {
      //STEP 3: Open a connection
      conn = createConnection();

      stmt = createStatement(conn);

      Instant start = Instant.now();
      try {
        stmt.executeUpdate("drop table if exists names");
        String sql = "CREATE TABLE names "
          + "(id INTEGER not NULL, "
          + " ts DATETIME, "
          + " PRIMARY KEY ( id ))";
        stmt.executeUpdate(sql);
        System.out.println("Successful creation of db table");
      } catch (Exception ex) {
        System.out.println("Exception in db ops\n"+ex);
        System.exit(1);
      }
      System.out.println("Time taken to create database: {} ms " + Duration.between(start, Instant.now()).toMillis());

      // Query the count continuously.
      long i = 1000;
      while (true) {
        try {
//          LOG.info("About to execute batch: {}", ++i);
          //start = Instant.now();
          stmt.execute("insert into names values (" + i + ",now());" +
            "delete from names where id =" + (i - 1) + ";" +
            "select count(*) from names");
          //LOG.info("Elapsed = {} ms", Duration.between(start, Instant.now()).toMillis());
        } catch (Exception ex) {
          System.out.println("Caught exception\n"+ex);
          System.out.println("Probing if the connection is valid... ");
          if (!conn.isValid(0)) {
            System.out.println("Connection is not valid, reconnecting...");
            conn = createConnection();
          }
          System.out.println("Have a valid connection now.");
          System.out.println("===============");
          System.out.println("Connected to: " + queryInstanceId(conn));
          System.out.println("===============");
          stmt = createStatement(conn);
        }
      }
//
//            stmt.executeUpdate("drop table if exists names");
//            System.out.println("Deleted table in a given database...");
    } catch (Exception ex) {
      System.out.println("Uncaught exception. Terminating.\n"+ex);
    } finally {
      if (stmt != null) {
        conn.close();
      }
      if (conn != null) {
        conn.close();
      }
    }
  }
}