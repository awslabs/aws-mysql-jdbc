package com.mysql.cj.conf.url;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.ConnectionUrlParser;

import java.util.Properties;

public class AwsMultiHostConnectionUrl extends ConnectionUrl {

  public AwsMultiHostConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
    super(connStrParser, info);
    this.type = ConnectionUrl.Type.MULTI_HOST_CONNECTION_AWS;
  }
}
