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

package testsuite.simple;

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.a.authentication.AwsIamAuthenticationPlugin;
import com.mysql.cj.protocol.a.authentication.AwsIamAuthenticationTokenHelper;
import com.mysql.cj.protocol.a.authentication.AwsIamClearAuthenticationPlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class AwsIamAuthHelperTest {
    final Log mockLog = Mockito.mock(Log.class);
    private static final String DB_CONN_STR = "localhost";
    private static final String TEST_USERNAME = "jane_doe";
    private static final String TEST_PASSWORD = "password";
    private static final int PORT = 3306;

    // To Access Protected Methods & Variables of AwsIamAuthenticationTokenHelper
    public class AwsIamHelperMock extends AwsIamAuthenticationTokenHelper{
        public AwsIamHelperMock(String hostname, int port) {
            super(hostname, port);
        }

        public String getToken(){
            return super.token;
        }

        @Override
        public String generateAuthenticationToken(String user) {
            return super.generateAuthenticationToken(user);
        }

        @Override
        public String getRdsRegion() {
            return super.getRdsRegion();
        }
    }

    // To Access Protected Methods & Variables of AwsIamAuthenticationPlugin
    public class AwsIamPluginMock extends AwsIamAuthenticationPlugin{
        public AwsIamPluginMock(AwsIamAuthenticationTokenHelper helper) {
            super(helper);
        }

        public String getPassword(){
            return super.password;
        }
    }

    // To Access Protected Methods & Variables
    public class AwsIamClearPluginMock extends AwsIamClearAuthenticationPlugin{
        public AwsIamClearPluginMock(AwsIamAuthenticationTokenHelper helper) {
            super(helper);
        }

        public String getPassword(){
            return super.password;
        }
    }

    @Test
    void testGenerateToken() {
        try(MockedConstruction<AwsIamHelperMock> mockedConstruction = Mockito.mockConstruction(AwsIamHelperMock.class)) {
            AwsIamHelperMock mockHelper = new AwsIamHelperMock(DB_CONN_STR, PORT);
            MockitoAnnotations.openMocks(this);
            Mockito.when(mockHelper.generateAuthenticationToken(TEST_USERNAME)).thenReturn("SOME_PASSWORD");

            AwsIamPluginMock awsIamPlugin = new AwsIamPluginMock(mockHelper);
            AwsIamClearPluginMock awsIamClearPlugin = new AwsIamClearPluginMock(mockHelper);

            Assertions.assertNull(mockHelper.getToken());
            Assertions.assertNull(awsIamPlugin.getPassword());
            Assertions.assertNull(awsIamClearPlugin.getPassword());

            // First Time Token Generation
            awsIamPlugin.setAuthenticationParameters(TEST_USERNAME, TEST_PASSWORD);
            Assertions.assertEquals("SOME_PASSWORD", mockHelper.getToken());

            // Second Call using same Helper should NOT generate a new token
            Mockito.when(mockHelper.generateAuthenticationToken(TEST_USERNAME)).thenReturn("WRONG_PASSWORD");
            awsIamClearPlugin.setAuthenticationParameters(TEST_USERNAME, TEST_PASSWORD);
            Assertions.assertEquals("SOME_PASSWORD", mockHelper.getToken());

            // Both Should have same token/password
            Assertions.assertEquals("SOME_PASSWORD", awsIamPlugin.getPassword());
            Assertions.assertEquals("SOME_PASSWORD", awsIamClearPlugin.getPassword());
        }
    }

    @Nested
    class ValidHostAndRegions{
        @ParameterizedTest
        @ValueSource(strings = {"us-east-1", "us-east-2"})
        void validAwsIamRdsHostAndRegion(String reg){
            Assertions.assertNotNull(new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName." + reg + ".rds.amazonaws.com", PORT));
        }
    }

    @Nested
    class InvalidHost{
        @ParameterizedTest
        @ValueSource(strings = {"notRDS", "rrr"})
        void invalidAwsIamNotRdsHost(String host) {
            Assertions.assertThrows(CJException.class, () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2." + host + ".amazonaws.com", PORT));
        }

        @ParameterizedTest
        @ValueSource(strings = {"notamazon", "amazon"})
        void invalidAwsIamNotAmazonHost(String host) {
            Assertions.assertThrows(CJException.class, () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2.rds." + host + ".com", PORT));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", " "})
        void invalidAwsIamEmptyHost(String host) {
            Assertions.assertThrows(CJException.class, () -> new AwsIamAuthenticationTokenHelper(host, PORT));
        }

        @Nested
        class UsingIP{
            @ParameterizedTest
            @ValueSource(strings = {"192.168.0.1", "localhost"})
            void invalidAwsIamUsingIP(String host) {
                Assertions.assertThrows(CJException.class, () -> new AwsIamAuthenticationTokenHelper(host, PORT));
            }
        }
    }

    @Nested
    class InvalidRegion{
        @ParameterizedTest
        @ValueSource(strings = {"usa-east-1", "random", "", " ", "us-est-2"})
        void invalidAwsIamInvalidRegion(String reg) {
            Assertions.assertThrows(CJException.class, () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName." + reg + ".rds.amazonaws.com", PORT));
        }
    }


}
