/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0
 * (GPLv2), as published by the Free Software Foundation, with the
 * following additional permissions:
 *
 * This program is distributed with certain software that is licensed
 * under separate terms, as designated in a particular file or component
 * or in the license documentation. Without limiting your rights under
 * the GPLv2, the authors of this program hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with the program.
 *
 * Without limiting the foregoing grant of rights under the GPLv2 and
 * additional permission as to separately licensed software, this
 * program is also subject to the Universal FOSS Exception, version 1.0,
 * a copy of which can be found along with its FAQ at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html.
 */

package com.mysql.cj.jdbc.ha.util;

import com.mysql.cj.util.StringUtils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RdsUtils {

  // Aurora DB clusters support different endpoints. More details about Aurora RDS endpoints
  // can be found at
  // https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.Endpoints.html
  //
  // Details how to use RDS Proxy endpoints can be found at
  // https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html
  //
  // Values like "<...>" depend on particular Aurora cluster.
  // For example: "<database-cluster-name>"
  //
  //
  //
  // Cluster (Writer) Endpoint: <database-cluster-name>.cluster-<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres.cluster-123456789012.us-east-2.rds.amazonaws.com
  //
  // Cluster Reader Endpoint: <database-cluster-name>.cluster-ro-<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres.cluster-ro-123456789012.us-east-2.rds.amazonaws.com
  //
  // Cluster Custom Endpoint: <cluster-name-alias>.cluster-custom-<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres-alias.cluster-custom-123456789012.us-east-2.rds.amazonaws.com
  //
  // Instance Endpoint: <instance-name>.<xyz>.<aws-region>.rds.amazonaws.com
  // Example: test-postgres-instance-1.123456789012.us-east-2.rds.amazonaws.com
  //
  //
  //
  // Similar endpoints for China regions have different structure and are presented below.
  //
  // Cluster (Writer) Endpoint: <database-cluster-name>.cluster-<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres.cluster-123456789012.rds.cn-northwest-1.amazonaws.com.cn
  //
  // Cluster Reader Endpoint: <database-cluster-name>.cluster-ro-<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres.cluster-ro-123456789012.rds.cn-northwest-1.amazonaws.com.cn
  //
  // Cluster Custom Endpoint: <cluster-name-alias>.cluster-custom-<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres-alias.cluster-custom-123456789012.rds.cn-northwest-1.amazonaws.com.cn
  //
  // Instance Endpoint: <instance-name>.<xyz>.rds.<aws-region>.amazonaws.com.cn
  // Example: test-postgres-instance-1.123456789012.rds.cn-northwest-1.amazonaws.com.cn

  private static final Pattern AURORA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com(\\.cn)?)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com(\\.cn)?)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CUSTOM_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-custom-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com(\\.cn)?)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_PROXY_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>[a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com(\\.cn)?)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-|cluster-|cluster-ro-|cluster-custom-)?"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>rds\\.[a-zA-Z0-9\\-]+|"
              + "[a-zA-Z0-9\\-]+\\.rds)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern AURORA_CHINA_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-|cluster-ro-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>rds\\.[a-zA-Z0-9\\-]+|"
              + "[a-zA-Z0-9\\-]+\\.rds)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CHINA_CUSTOM_CLUSTER_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>cluster-custom-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>rds\\.[a-zA-Z0-9\\-]+|"
              + "[a-zA-Z0-9\\-]+\\.rds)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern AURORA_CHINA_PROXY_DNS_PATTERN =
      Pattern.compile(
          "(?<instance>.+)\\."
              + "(?<dns>proxy-)+"
              + "(?<domain>[a-zA-Z0-9]+\\.(?<region>rds\\.[a-zA-Z0-9\\-]+|"
              + "[a-zA-Z0-9\\-]+\\.rds)\\.amazonaws\\.com\\.cn)",
          Pattern.CASE_INSENSITIVE);

  private static final String DNS_GROUP = "dns";
  private static final String DOMAIN_GROUP = "domain";

  public static String getRdsInstanceHostPattern(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return "?";
    }

    final Matcher matcher = AURORA_DNS_PATTERN.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group(DOMAIN_GROUP);
    }
    final Matcher chinaMatcher = AURORA_CHINA_DNS_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return "?." + chinaMatcher.group(DOMAIN_GROUP);
    }
    return "?";
  }

  public static String getRdsClusterHostUrl(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return null;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return host.replaceAll(AURORA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    final Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return host.replaceAll(AURORA_CHINA_CLUSTER_PATTERN.pattern(), "${instance}.cluster-${domain}");
    }
    return null;
  }

  public static boolean isReaderClusterDns(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }

    final Matcher matcher = AURORA_CLUSTER_PATTERN.matcher(host);
    if (matcher.find()) {
      return "cluster-ro-".equalsIgnoreCase(matcher.group(DNS_GROUP));
    }
    final Matcher chinaMatcher = AURORA_CHINA_CLUSTER_PATTERN.matcher(host);
    if (chinaMatcher.find()) {
      return "cluster-ro-".equalsIgnoreCase(chinaMatcher.group(DNS_GROUP));
    }
    return false;
  }

  public static boolean isRdsClusterDns(final String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_CLUSTER_PATTERN.matcher(host).find()
        || AURORA_CHINA_CLUSTER_PATTERN.matcher(host).find());
  }

  public static boolean isRdsCustomClusterDns(final String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_CUSTOM_CLUSTER_PATTERN.matcher(host).find()
        || AURORA_CHINA_CUSTOM_CLUSTER_PATTERN.matcher(host).find());
  }

  public static boolean isRdsDns(final String host) {
    return !StringUtils.isNullOrEmpty(host)
        && (AURORA_DNS_PATTERN.matcher(host).find()
        || AURORA_CHINA_DNS_PATTERN.matcher(host).find());
  }

  public static boolean isRdsProxyDns(final String host) {
    if (StringUtils.isNullOrEmpty(host)) {
      return false;
    }
    return AURORA_PROXY_DNS_PATTERN.matcher(host).find()
        || AURORA_CHINA_PROXY_DNS_PATTERN.matcher(host).find();
  }

  public static boolean isDnsPatternValid(final String pattern) {
    return pattern.contains("?");
  }
}
