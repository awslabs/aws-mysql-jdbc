package com.mysql.cj.util;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;

import java.util.Map;
import java.util.Properties;

public class ClusterAwareUtils {
    /**
     * Create a copy of the given {@link HostInfo} object where all details are the same except for the host properties,
     * which are set to the given map of properties
     *
     * @param baseHostInfo The {@link HostInfo} object to copy
     * @param props The map of properties to add to the new {@link HostInfo} object
     *
     * @return A copy of the given {@link HostInfo} object where all details are the same except for the host properties,
     *      which are set to the given map of properties
     */
    public static HostInfo hostInfoCopyWithNewProps(HostInfo baseHostInfo, Map<String, String> props) {
        DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(baseHostInfo.getDatabaseUrl(), convertMapToProperties(props));
        return new HostInfo(urlContainer, baseHostInfo.getHost(), baseHostInfo.getPort(), baseHostInfo.getUser(), baseHostInfo.getPassword(), props);
    }

    /**
     * Converts the format of the given properties from a {@link Map} to a {@link Properties} object
     *
     * @param mapProps A {@link Map} representation of a set of properties
     *
     * @return A {@link Properties} object containing the same entries as the given map
     */
    public static Properties convertMapToProperties(Map<String, String> mapProps) {
        Properties props = new Properties();
        for(Map.Entry<String, String> mapProp : mapProps.entrySet()) {
            props.setProperty(mapProp.getKey(), mapProp.getValue());
        }
        return props;
    }
}
