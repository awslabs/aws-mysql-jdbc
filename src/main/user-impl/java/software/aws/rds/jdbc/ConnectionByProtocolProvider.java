package software.aws.rds.jdbc;

import com.mysql.cj.conf.ConnectionUrl;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionByProtocolProvider {

    Connection getAwsProtocolConnection(ConnectionUrl connUrl) throws SQLException;

    Connection getReplicationProtocolConnection(ConnectionUrl connUrl) throws SQLException;
}
