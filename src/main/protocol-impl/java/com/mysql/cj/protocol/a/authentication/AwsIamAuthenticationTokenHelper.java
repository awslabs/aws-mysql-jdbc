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

package com.mysql.cj.protocol.a.authentication;

import com.mysql.cj.jdbc.ha.util.RdsUtils;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsIamAuthenticationTokenHelper {

  private String token;
  private final Region region;
  private final String hostname;
  private final int port;
  private final Log log;
  private static final String REGION_MATCHER_GROUP = "region";

  public AwsIamAuthenticationTokenHelper(final String hostname, final int port, final String logger) {
    this.log = LogFactory.getLogger(logger, Log.LOGGER_INSTANCE_NAME);
    this.hostname = hostname;
    this.port = port;
    this.region = getRdsRegion();
  }

  public String getOrGenerateToken(final String user) {
    if (this.token == null) {
      this.token = generateAuthenticationToken(user);
    }

    return token;
  }

  private String generateAuthenticationToken(final String user) {
    RdsUtilities utilities = RdsUtilities.builder()
      .credentialsProvider(DefaultCredentialsProvider.create())
      .region(this.region)
      .build();

    return utilities.generateAuthenticationToken((builder) ->
      builder
        .hostname(hostname)
        .port(port)
        .username(user)
    );
  }

  private Region getRdsRegion() {
    // Check Hostname
    Matcher matcher = RdsUtils.AURORA_DNS_PATTERN.matcher(hostname);
    if (!matcher.find()) {
      final Matcher chinaMatcher = RdsUtils.AURORA_CHINA_DNS_PATTERN.matcher(hostname);
      if (!chinaMatcher.find()) {
        // Does not match Amazon's Hostname, throw exception
        final String exceptionMessage = Messages.getString(
            "AuthenticationAwsIamPlugin.UnsupportedHostname",
            new String[]{hostname});

        log.logTrace(exceptionMessage);
        throw ExceptionFactory.createException(exceptionMessage);
      }
      matcher = chinaMatcher;
    }

    // Get Region
    final String rdsRegion = matcher.group(REGION_MATCHER_GROUP) == null
        ? null
        : matcher.group(REGION_MATCHER_GROUP).replaceAll("rds", "").replaceAll("\\.", "");

    // Check Region
    Optional<Region> regionOptional = Region.regions().stream()
            .filter(r -> r.id().equalsIgnoreCase(rdsRegion))
            .findFirst();

    if (!regionOptional.isPresent()) {
      final String exceptionMessage = Messages.getString(
          "AuthenticationAwsIamPlugin.UnsupportedRegion",
          new String[]{hostname});

      log.logTrace(exceptionMessage);
      throw ExceptionFactory.createException(exceptionMessage);
    }
    return regionOptional.get();
  }
}
