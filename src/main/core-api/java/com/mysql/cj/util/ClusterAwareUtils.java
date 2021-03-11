package com.mysql.cj.util;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;

import java.util.Map;
import java.util.Properties;

public class ClusterAwareUtils {
    public static HostInfo copyHostInfoAndAddProps(HostInfo baseHostInfo, Map<String, String> props) {
        DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(baseHostInfo.getDatabaseUrl(), convertMapToProperties(props));
        return new HostInfo(urlContainer, baseHostInfo.getHost(), baseHostInfo.getPort(), baseHostInfo.getUser(), baseHostInfo.getPassword(), props);
    }

    public static Properties convertMapToProperties(Map<String, String> mapProps) {
        Properties props = new Properties();
        for(Map.Entry<String, String> mapProp : mapProps.entrySet()) {
            props.setProperty(mapProp.getKey(), mapProp.getValue());
        }
        return props;
    }
}
