/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0
 * (GPLv2), as published by the Free Software Foundation, with the
 * following additional permissions:
 *
 * This program is distributed with certain software that is licensed
 * under separate terms, as designated in a particular file or component
 * or in the license documentation. Without limiting your rights under
 * the GPLv2, the authors of this program hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with the program.
 *
 * Without limiting the foregoing grant of rights under the GPLv2 and
 * additional permission as to separately licensed software, this
 * program is also subject to the Universal FOSS Exception, version 1.0,
 * a copy of which can be found along with its FAQ at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see
 * http://www.gnu.org/licenses/gpl-2.0.html.
 */

package testsuite.simple;

import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.protocol.a.authentication.AwsIamAuthenticationTokenHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AwsIamAuthenticationHelperTest {

    private static final int PORT = 3306;

    /**
     *  Attempt to create new {@link AwsIamAuthenticationTokenHelper} with valid host name and region.
     */
    @Test
    public void test_1_ValidHostAndRegion() {
        Assertions.assertNotNull(new AwsIamAuthenticationTokenHelper(
            "MyDBInstanceName.SomeServerName.us-east-1.rds.amazonaws.com",
            PORT,
            StandardLogger.class.getName()
        ));
    }


    /**
     *  Attempt to create new {@link AwsIamAuthenticationTokenHelper} with invalid RDS format.
     */
    @Test
    public void test_2_NotRdsHost() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "MyDBInstanceName.SomeServerName.us-east-2.notRDS.amazonaws.com",
                PORT,
                StandardLogger.class.getName()
            )
        );
    }


    /**
     *  Attempt to create new {@link AwsIamAuthenticationTokenHelper} with invalid hostname.
     */
    @Test
    public void test_3_NotAmazonHost() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "MyDBInstanceName.SomeServerName.us-east-2.rds.notamazon.com",
                PORT,
                StandardLogger.class.getName()
            )
        );
    }


    /**
     *  Attempt to create new {@link AwsIamAuthenticationTokenHelper} with empty hostname.
     */
    @Test
    public void test_4_EmptyHost() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "",
                PORT,
                StandardLogger.class.getName()
            )
        );

        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                " ",
                PORT,
                StandardLogger.class.getName()
            )
        );
    }


    /**
     *  Attempt to create new {@link AwsIamAuthenticationTokenHelper} with invalid RDS format.
     */
    @Test
    public void test_5_InvalidHostUsingIP() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "192.168.0.1",
                PORT,
                StandardLogger.class.getName()
            )
        );

        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "localhost",
                PORT,
                StandardLogger.class.getName()
            )
        );
    }

    /**
     *  Attempt to create new {@link AwsIamAuthenticationTokenHelper} with invalid region.
     */
    @Test
    public void test_6_InvalidRegion() {
        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "MyDBInstanceName.SomeServerName.fake-1.rds.amazonaws.com",
                PORT,
                StandardLogger.class.getName()
            )
        );

        Assertions.assertThrows(
            CJException.class,
            () -> new AwsIamAuthenticationTokenHelper(
                "MyDBInstanceName.SomeServerName. .rds.amazonaws.com",
                PORT,
                StandardLogger.class.getName()
            )
        );
    }
}
