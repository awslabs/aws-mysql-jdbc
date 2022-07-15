/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package testsuite;

import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.mysql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class MyTestClass {

    @Test
    public void test_replication() throws SQLException {
        DriverManager.registerDriver(new Driver());
        String user = "admin";
        String pass = "my_password_2020";
        String url = "jdbc:mysql:replication://" +
                "mysql-instance-3.czygpppufgy4.us-east-2.rds.amazonaws.com," +
                "mysql-instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com," +
                "mysql-instance-2.czygpppufgy4.us-east-2.rds.amazonaws.com," +
                "mysql-instance-4.czygpppufgy4.us-east-2.rds.amazonaws.com," +
                "mysql-instance-5.czygpppufgy4.us-east-2.rds.amazonaws.com";
        try (Connection conn = DriverManager.getConnection(url, user, pass);
            Statement stmt = conn.createStatement()) {
            conn.setReadOnly(true);
            stmt.execute("SET autocommit = 0");
            stmt.executeQuery("SELECT 1");
            stmt.execute("COMMIT");
            stmt.executeQuery("SELECT 2");
            stmt.execute("COMMIT");
            stmt.executeQuery("SELECT 3");
        }
    }
}
