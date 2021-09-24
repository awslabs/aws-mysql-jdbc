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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsIamAuthenticationTokenHelper {

  protected String token;
  protected String region;
  protected String hostname;
  protected int port;

  public AwsIamAuthenticationTokenHelper(final String hostname, final int port) {
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

  protected String generateAuthenticationToken(String user) {
    final RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator
        .builder()
        .region(this.region)
        .credentials(new DefaultAWSCredentialsProviderChain())
        .build();

    return generator.getAuthToken(GetIamAuthTokenRequest
        .builder()
        .hostname(this.hostname)
        .port(this.port)
        .userName(user)
        .build());
  }

  protected String getRdsRegion() {
    // Check Hostname
    Pattern auroraDnsPattern =
        Pattern.compile(
            "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?[a-zA-Z0-9]+\\.([a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com",
            Pattern.CASE_INSENSITIVE);
    Matcher matcher = auroraDnsPattern.matcher(hostname);
    if (!matcher.find()) {
      // Does not match Amazon's Hostname, throw exception
      throw ExceptionFactory.createException(Messages.getString(
              "AuthenticationAwsIamPlugin.UnsupportedHostname",
              new String[]{hostname})
      );
    }

    // Get and Check Region
    String retReg = matcher.group(3);
    try {
      Regions.fromName(retReg);
    } catch (IllegalArgumentException exception) {

      throw ExceptionFactory.createException(
          Messages.getString(
              "AuthenticationAwsIamPlugin.UnsupportedHostname",
              new String[]{hostname})
          );
    }
    return retReg;
  }
}
