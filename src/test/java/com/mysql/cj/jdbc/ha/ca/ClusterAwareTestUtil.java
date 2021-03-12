package com.mysql.cj.jdbc.ha.ca;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClusterAwareTestUtil {
    protected static HostInfo createBasicHostInfo(String instanceName, String db) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(TopologyServicePropertyKeys.INSTANCE_NAME, instanceName);
        String url = "jdbc:mysql:aws://" + instanceName + ".com:1234/";
        db = db == null ? "" : db;
        properties.put(PropertyKey.DBNAME.getKeyName(), db);
        url += db;
        final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
        return new HostInfo(conStr, instanceName, 1234, null, null, properties);
    }
}
