package testsuite.simple;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.a.authentication.AwsIamAuthenticationPlugin;
import com.mysql.cj.protocol.a.authentication.AwsIamClearAuthenticationPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Properties;

public class AwsIamAuthTest {
    private static final String DB_CONN_STR = System.getenv("DB_CONN_STR");
    private static final String TEST_USERNAME = "jane_doe";
    private static final String TEST_PASSWORD = "password";
    private static final int PORT = 3306;

    @Nested
    class AwsIamPlugin {
        @ParameterizedTest
        @ValueSource(strings = {"us-gov-west-1", "us-gov-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1", "eu-south-1", "ap-east-1", "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "sa-east-1", "cn-north-1", "cn-northwest-1", "ca-central-1", "me-south-1", "af-south-1"})
        void validAwsIamRdsHostAndRegion(String reg) {
            Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName." + reg + ".rds.amazonaws.com", PORT));
        }

        @Test
        void invalidAwsIamNotRdsHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.notRDS.amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rrr.amazonaws.com", PORT))
            );
        }

        @Test
        void invalidAwsIamNotAmazonHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.notamazon.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.amazon.com", PORT))
            );
        }

        @Test
        void invalidAwsIamUsingIP() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin(hostToIP("us-east-2.console.aws.amazon.com"), PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin(hostToIP(DB_CONN_STR.substring("jdbc:mysql://".length())), PORT))
            );
        }

        @Test
        void invalidAwsIamEmptyHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin(" ", PORT))
            );
        }

        @Test
        void invalidAwsIamInvalidRegion() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.usa-east-1.rds.amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.random.rds.amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName..amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-est-2.rds.amazonaws.com", PORT))
            );
        }
    }

    @Nested
    class AwsIamClearPlugin {
        @ParameterizedTest
        @ValueSource(strings = {"us-gov-west-1", "us-gov-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1", "eu-north-1", "eu-south-1", "ap-east-1", "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-northeast-1", "ap-northeast-2", "sa-east-1", "cn-north-1", "cn-northwest-1", "ca-central-1", "me-south-1", "af-south-1"})
        void validAwsIamRdsHostAndRegion(String reg) {
            Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName." + reg + ".rds.amazonaws.com", PORT));
        }

        @Test
        void invalidAwsIamNotRdsHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.notRDS.amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rrr.amazonaws.com", PORT))
            );
        }

        @Test
        void invalidAwsIamNotAmazonHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.notamazon.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.amazon.com", PORT))
            );
        }

        @Test
        void invalidAwsIamUsingIP() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin(hostToIP("us-east-2.console.aws.amazon.com"), PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin(hostToIP(DB_CONN_STR.substring("jdbc:mysql://".length())), PORT))
            );
        }

        @Test
        void invalidAwsIamEmptyHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin(" ", PORT))
            );
        }

        @Test
        void invalidAwsIamInvalidRegion() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.usa-east-1.rds.amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.random.rds.amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName..amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName. .amazonaws.com", PORT)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-est-2.rds.amazonaws.com", PORT))
            );
        }
    }

    @Nested
    class AwsIamAuthConnection {
        @Test
        void validConnectionIam() throws SQLException, ClassNotFoundException {
            // Load Driver
            Class.forName("software.aws.rds.jdbc.mysql.Driver");

            // Set Properties for User/Pass & to use IAM
            Properties properties = new Properties();
            properties.setProperty("useIAM", Boolean.TRUE.toString());
            properties.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
            properties.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);

            // Try a Connection & Query
            Connection conn = DriverManager.getConnection(DB_CONN_STR, properties);
            Statement stmt1 = conn.createStatement();
            ResultSet rs = stmt1.executeQuery("SELECT 1");
            while (rs.next()) {
                Assertions.assertEquals("1", rs.getString(1));
            }
        }

        @Test
        void invalidConnectionIamWrongUser() throws ClassNotFoundException {
            // Load Driver
            Class.forName("software.aws.rds.jdbc.mysql.Driver");

            // Set Properties for User/Pass & to use IAM
            Properties properties = new Properties();
            properties.setProperty("useIAM", Boolean.TRUE.toString());
            properties.setProperty(PropertyKey.USER.getKeyName(), "NOT_A_VALID_USER");
            properties.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);

            Assertions.assertThrows(SQLException.class, () -> {
                Connection conn = DriverManager.getConnection(DB_CONN_STR, properties);
            });
        }

        @Test
        void invalidConnectionIamIpHostname() throws ClassNotFoundException {
            // Load Driver
            Class.forName("software.aws.rds.jdbc.mysql.Driver");

            // Set Properties for User/Pass & to use IAM
            Properties properties = new Properties();
            properties.setProperty("useIAM", Boolean.TRUE.toString());
            properties.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
            properties.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);

            // Using IP Address as Hostname
            String hostname = DB_CONN_STR.substring("jdbc:mysql://".length());

            Assertions.assertThrows(SQLException.class, () -> {
                Connection conn = DriverManager.getConnection(hostToIP(hostname), properties);
            });
        }
    }

    private String hostToIP(String hostname){
        InetAddress inet = null;
        try {
            inet = InetAddress.getByName(hostname);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return inet.getHostAddress();
    }
}