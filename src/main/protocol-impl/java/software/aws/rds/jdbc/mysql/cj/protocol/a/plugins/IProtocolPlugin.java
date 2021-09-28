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

package software.aws.rds.jdbc.mysql.cj.protocol.a.plugins;

import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.SocketConnection;


import java.io.IOException;

public interface IProtocolPlugin {
  void init(PropertySet propertySet, HostInfo hostInfo, IProtocolPlugin next, Log log);

  void beforeHandshake(SocketConnection socketConnection) throws IOException;
  void afterHandshake(SocketConnection socketConnection) throws IOException;

  void beforeCommand(IReadContext context) throws IOException;
  void beforeRead(IReadContext context) throws IOException;
  void negotiateReadTimeouts(IReadTimeoutHolder timeoutHolder);
  void beforeReadPortion(IReadContext context) throws IOException;
  boolean readNextPortion(IReadContext context) throws IOException;
  void readPortionInterceptException(IReadContext context, Throwable throwable) throws IOException;
  Object afterReadPortion(IReadContext context, Object payload) throws IOException;
  void afterRead(IReadContext context) throws IOException;
  void afterCommand(IReadContext context) throws IOException;

  void releaseResources();
}
