package testsuite.simple;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.a.authentication.AwsIamAuthenticationPlugin;
import com.mysql.cj.protocol.a.authentication.AwsIamClearAuthenticationPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Properties;

public class AwsIamAuthTest {
    private static final String DB_CONN_STR = System.getenv("DB_CONN_STR");
    private static final String TEST_USERNAME = "jane_doe";
    private static final String TEST_PASSWORD = "password";
    private static final int port = 3306;

    @Nested
    class AwsIamPlugin {
        @Test
        void validAwsIamRdsHostAndRegion() {
            Assertions.assertAll(
                    // Using all Regions from
                    // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.af-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-northeast-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-northeast-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-northeast-3.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-southeast-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-southeast-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.ca-central-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.cn-north-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.cn-northwest-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-central-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-north-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-west-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-west-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-west-3.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.govcloud.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.me-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.sa-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-gov-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-iso-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-iso-west-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-isob-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-west-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-west-2.rds.amazonaws.com", port))
            );
        }

        @Test
        void invalidAwsIamNotRdsHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.notRDS.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rrr.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.dsr.amazonaws.com", port))
            );
        }

        @Test
        void invalidAwsIamNotAmazonHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.notamazon.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.amazon.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.google.com", port))
            );
        }

        @Test
        void invalidAwsIamUsingIP() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin(hostToIP("us-east-2.console.aws.amazon.com"), port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin(hostToIP(DB_CONN_STR.substring("jdbc:mysql://".length())), port))
            );
        }

        @Test
        void invalidAwsIamEmptyHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin(" ", port))
            );
        }

        @Test
        void invalidAwsIamInvalidRegion() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.usa-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.random.rds.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName..amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-est-2.rds.amazonaws.com", port))
            );
        }
    }

    @Nested
    class AwsIamClearPlugin {
        @Test
        void validAwsIamRdsHostAndRegion() {
            Assertions.assertAll(
                    // Using all Regions from
                    // https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.af-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-northeast-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-northeast-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-northeast-3.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-southeast-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ap-southeast-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.ca-central-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.cn-north-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.cn-northwest-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-central-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-north-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-west-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-west-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.eu-west-3.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.govcloud.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.me-south-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.sa-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-gov-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-iso-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-iso-west-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-isob-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-west-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertNotNull(new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-west-2.rds.amazonaws.com", port))
            );
        }

        @Test
        void invalidAwsIamNotRdsHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.notRDS.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rrr.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.dsr.amazonaws.com", port))
            );
        }

        @Test
        void invalidAwsIamNotAmazonHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.notamazon.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.amazon.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-east-2.rds.google.com", port))
            );
        }

        @Test
        void invalidAwsIamUsingIP() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin(hostToIP("us-east-2.console.aws.amazon.com"), port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin(hostToIP(DB_CONN_STR.substring("jdbc:mysql://".length())), port))
            );
        }

        @Test
        void invalidAwsIamEmptyHost() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin(" ", port))
            );
        }

        @Test
        void invalidAwsIamInvalidRegion() {
            Assertions.assertAll(
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.usa-east-1.rds.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.random.rds.amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName..amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName. .amazonaws.com", port)),
                    () -> Assertions.assertThrows(WrongArgumentException.class, () -> new AwsIamClearAuthenticationPlugin("MyDBInstanceName.SomeServerName.us-est-2.rds.amazonaws.com", port))
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