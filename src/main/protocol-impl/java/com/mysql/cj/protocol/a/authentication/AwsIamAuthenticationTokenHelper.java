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

  private String token;
  private String region;
  private String hostname;
  private int port;

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

  private String generateAuthenticationToken(String user) {
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

  private String getRdsRegion() {
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
              new String[]{hostname}));
    }

    // Get and Check Region
    String retReg = matcher.group(3);
    try {
      Regions.fromName(retReg);
    } catch (IllegalArgumentException e) {
      throw ExceptionFactory.createException(Messages.getString(
              "AuthenticationAwsIamPlugin.UnsupportedRegion",
              new String[]{retReg}));
    }
    return retReg;
  }
}
