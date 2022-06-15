// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0
// (GPLv2), as published by the Free Software Foundation, with the
// following additional permissions:
//
// This program is distributed with certain software that is licensed
// under separate terms, as designated in a particular file or component
// or in the license documentation. Without limiting your rights under
// the GPLv2, the authors of this program hereby grant you an additional
// permission to link the program and your derivative works with the
// separately licensed software that they have included with the program.
//
// Without limiting the foregoing grant of rights under the GPLv2 and
// additional permission as to separately licensed software, this
// program is also subject to the Universal FOSS Exception, version 1.0,
// a copy of which can be found along with its FAQ at
// http://oss.oracle.com/licenses/universal-foss-exception.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see
// http://www.gnu.org/licenses/gpl-2.0.html.

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.conf.HostInfo;

public class RdsHost {
  static final int NO_PORT = -1;
  static final String HOST_PORT_SEPARATOR = ":";
  private final RdsUrlType rdsUrlType;
  private final String clusterId;
  private final HostInfo clusterInstanceTemplate;
  private final String host;
  private final int port;

  public RdsHost(RdsUrlType rdsUrlType, String clusterId, HostInfo clusterInstanceTemplate, String host, int port) {
    this.rdsUrlType = rdsUrlType;
    this.clusterId = clusterId;
    this.clusterInstanceTemplate = clusterInstanceTemplate;
    this.host = host;
    this.port = port;
  }

  public RdsUrlType getUrlType() {
    return this.rdsUrlType;
  }

  public String getClusterId() {
    return this.clusterId;
  }

  public HostInfo getClusterInstanceTemplate() {
    return this.clusterInstanceTemplate;
  }

  public String getHost() {
    return this.host;
  }

  public int getPort() {
    return this.port;
  }

  public String getHostPortPair() {
    return this.host + HOST_PORT_SEPARATOR + this.port;
  }
}
