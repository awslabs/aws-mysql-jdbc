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

import com.mysql.cj.protocol.Security;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;

import java.util.List;

public class AwsIamAuthenticationPlugin extends AwsIamAuthenticationBasePlugin {

  public AwsIamAuthenticationPlugin(String hostname, int port) {
    super(hostname, port);
  }

  public String getProtocolPluginName() {
    return "aws_iam_authentication";
  }

  @Override
  public boolean nextAuthenticationStep(
      NativePacketPayload fromServer, List<NativePacketPayload> toServer) {

    toServer.clear();

    NativePacketPayload bresp;

    String pwd = this.authenticationToken;

    if (fromServer == null || pwd == null || pwd.length() == 0) {
      bresp = new NativePacketPayload(new byte[0]);
    } else {
      bresp = new NativePacketPayload(
          Security.scramble411(pwd, fromServer.readBytes(StringSelfDataType.STRING_TERM), this.protocol.getPasswordCharacterEncoding()));
    }
    toServer.add(bresp);

    return true;
  }
}
