/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package com.mysql.cj.protocol.a.authentication;

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
  private static final int REGION_MATCHER_GROUP = 3;

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
    final Pattern auroraDnsPattern =
        Pattern.compile(
            "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?[a-zA-Z0-9]+\\.([a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com",
            Pattern.CASE_INSENSITIVE);
    final Matcher matcher = auroraDnsPattern.matcher(hostname);
    if (!matcher.find()) {
      // Does not match Amazon's Hostname, throw exception
      final String exceptionMessage = Messages.getString(
          "AuthenticationAwsIamPlugin.UnsupportedHostname",
          new String[]{hostname});

      log.logTrace(exceptionMessage);
      throw ExceptionFactory.createException(exceptionMessage);
    }

    // Get Region
    final String rdsRegion = matcher.group(REGION_MATCHER_GROUP);

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
