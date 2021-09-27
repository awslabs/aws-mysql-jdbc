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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class AwsIamAuthenticationHelperTest {

    private static final int PORT = 3306;

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with valid host name and region
     */
    @Test
    public void test_1_ValidHostAndRegion() {
        Assertions.assertNotNull(new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2.rds.amazonaws.com", PORT));
    }

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with invalid RDS format
     */
    @Test
    public void test_2_NotRdsHost() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "MyDBInstanceName.SomeServerName.us-east-2.notRDS.amazonaws.com", PORT));
    }

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with invalid hostname
     */
    @Test
    public void test_3_NotAmazonHost() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2.rds.notAmazon.com", PORT));
    }

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with empty hostname
     */
    @Test
    public void test_4_EmptyHost() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.us-east-2.rds. .com", PORT));
    }

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with IP Address as hostname
     */
    @Test
    public void test_5_InvalidHostUsingIP() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper("192.168.0.1", PORT));
    }

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with invalid region
     */
    @Test
    public void test_6_InvalidRegion() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName.fake-region.rds.amazonaws.com", PORT));
    }

    /**
     *  Attempt to create new AWSIAMAuthenticationHelper with empty region
     */
    @Test
    public void test_7_EmptyRegion() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper("MyDBInstanceName.SomeServerName..rds.amazonaws.com", PORT));
    }
}