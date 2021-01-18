package software.aws.rds.jdbc;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;

import java.sql.Connection;
import java.sql.SQLException;

public class StandardConnectionByProtocolProvider implements ConnectionByProtocolProvider {

    @Override
    public Connection getAwsProtocolConnection(ConnectionUrl connUrl) throws SQLException {
        return ClusterAwareConnectionProxy.autodetectClusterAndCreateProxyInstance(connUrl);
    }

    @Override
    public Connection getReplicationProtocolConnection(ConnectionUrl connUrl) throws SQLException {
        return ReplicationConnectionProxy.createProxyInstance(connUrl);
    }
}
