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
import com.mysql.cj.protocol.a.authentication.AwsIamAuthenticationTokenHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AwsIamAuthenticationHelperTest {
    private static final int PORT = 3306;

    @Nested
    class ValidHostAndRegions{
        @ParameterizedTest
        @ValueSource(strings = {"us-east-1", "us-east-2"})
        void validAwsIamRdsHostAndRegion(String reg) {
            Assertions.assertNotNull(new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName." + reg + ".rds.amazonaws.com", PORT));
        }
    }

    @Nested
    class InvalidHost{
        @ParameterizedTest
        @ValueSource(strings = {"notRDS", "rrr"})
        void invalidAwsIamNotRdsHost(String host) {
            Assertions.assertThrows(
                CJException.class,
                () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2." + host + ".amazonaws.com", PORT));
        }

        @ParameterizedTest
        @ValueSource(strings = {"notamazon", "amazon"})
        void invalidAwsIamNotAmazonHost(String host) {
            Assertions.assertThrows(
                CJException.class,
                () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2.rds." + host + ".com", PORT));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", " "})
        void invalidAwsIamEmptyHost(String host) {
            Assertions.assertThrows(
                CJException.class,
                () -> new AwsIamAuthenticationTokenHelper(host, PORT));
        }

        @Nested
        class UsingIP{
            @ParameterizedTest
            @ValueSource(strings = {"192.168.0.1", "localhost"})
            void invalidAwsIamUsingIP(String host) {
                Assertions.assertThrows(
                    CJException.class,
                    () -> new AwsIamAuthenticationTokenHelper(host, PORT));
            }
        }
    }

    @Nested
    class InvalidRegion{
        @ParameterizedTest
        @ValueSource(strings = {"usa-east-1", "random", "", " ", "us-est-2"})
        void invalidAwsIamInvalidRegion(String reg) {
            Assertions.assertThrows(
                CJException.class,
                () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName." + reg + ".rds.amazonaws.com", PORT));
        }
    }
}
