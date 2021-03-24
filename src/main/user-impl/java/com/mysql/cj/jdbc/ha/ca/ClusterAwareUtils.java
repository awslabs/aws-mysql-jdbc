package com.mysql.cj.jdbc.ha.ca;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.DatabaseUrlContainer;
import com.mysql.cj.conf.HostInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClusterAwareUtils {
    /**
     * Create a copy of the given {@link HostInfo} object where all details are the same except for the host properties,
     * which will contain both the original properties and the properties passed into the function
     *
     * @param baseHostInfo The {@link HostInfo} object to copy
     * @param additionalProps The map of properties to add to the new {@link HostInfo} copy
     *
     * @return A copy of the given {@link HostInfo} object where all details are the same except for the host properties,
     *      will contain both the original properties and the properties passed into the function
     */
    public static HostInfo copyWithAdditionalProps(HostInfo baseHostInfo, Map<String, String> additionalProps) {
        DatabaseUrlContainer urlContainer = ConnectionUrl.getConnectionUrlInstance(baseHostInfo.getDatabaseUrl(), new Properties());
        Map<String, String> originalProps = baseHostInfo.getHostProperties();
        Map<String, String> mergedProps = new HashMap<>();
        mergedProps.putAll(originalProps);
        mergedProps.putAll(additionalProps);
        return new HostInfo(urlContainer, baseHostInfo.getHost(), baseHostInfo.getPort(), baseHostInfo.getUser(), baseHostInfo.getPassword(), mergedProps);
    }
}
