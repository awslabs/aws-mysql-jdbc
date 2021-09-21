package testsuite.smoke;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator.Builder;
import com.mysql.cj.conf.PropertyKey;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SmokeTest {

  private static final String CONNECTION_STRING = "jdbc:mysql:aws://database-mysql-instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com";
//  private static final String USERNAME = "jane_doe";
  private static final String USERNAME = "jane_doe";
  private static final String PASSWORD = "my_password_2020";

  private static final String RDS_INSTANCE_HOSTNAME = "database-mysql-instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com";
  private static final int RDS_INSTANCE_PORT = 3306;
  private static final String REGION_NAME = "us-east-2";
  private static final String DB_USER = "jane_doe";

  private static final Builder TOKEN_GENERATOR = RdsIamAuthTokenGenerator
      .builder()
      .region(REGION_NAME);

  public static void main(String[] args) throws ClassNotFoundException {
    Class.forName("software.aws.rds.jdbc.mysql.Driver");

    Properties properties = new Properties();
    properties.setProperty("useIAM", Boolean.TRUE.toString());

    properties.setProperty(PropertyKey.USER.getKeyName(), USERNAME);
    properties.setProperty(PropertyKey.PASSWORD.getKeyName(), PASSWORD);

    try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
      try (Statement stmt1 = conn.createStatement()) {
        try (ResultSet rs = stmt1.executeQuery("SELECT 1")) {
          while (rs.next()) {
            System.out.println(rs.getString(1));
          }
        }
      }
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }

  public static String generateAuthToken() {
    RdsIamAuthTokenGenerator generator = TOKEN_GENERATOR
        .credentials(new DefaultAWSCredentialsProviderChain())
        .build();

    return generator.getAuthToken(GetIamAuthTokenRequest
        .builder()
        .hostname(RDS_INSTANCE_HOSTNAME)
        .port(RDS_INSTANCE_PORT)
        .userName(DB_USER)
        .build());
  }

}
