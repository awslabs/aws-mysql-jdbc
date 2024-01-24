/*
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.jdbc.ha.util.RdsUtils;
import org.junit.jupiter.api.Test;

public class RdsUtilsTests {

  private static final String usEastCluster =
      "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastInstance =
      "instance-test-name.XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastProxy =
      "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastCustomDomain =
      "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";

  private static final String chinaCluster =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaInstance =
      "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaProxy =
      "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaCustomDomain =
      "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn";

  private static final String oldChinaCluster =
      "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaInstance =
      "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaProxy =
      "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaCustomDomain =
      "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn";

  private static final String extraRdsChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String missingCnChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com";
  private static final String missingRegionChinaPath =
      "database-test-name.cluster-XYZ.rds.amazonaws.com.cn";

  @Test
  public void testIsRdsDns() {
    assertTrue(RdsUtils.isRdsDns(usEastCluster));
    assertTrue(RdsUtils.isRdsDns(usEastClusterReadOnly));
    assertTrue(RdsUtils.isRdsDns(usEastInstance));
    assertTrue(RdsUtils.isRdsDns(usEastProxy));
    assertTrue(RdsUtils.isRdsDns(usEastCustomDomain));

    assertTrue(RdsUtils.isRdsDns(chinaCluster));
    assertTrue(RdsUtils.isRdsDns(chinaClusterReadOnly));
    assertTrue(RdsUtils.isRdsDns(chinaInstance));
    assertTrue(RdsUtils.isRdsDns(chinaProxy));
    assertTrue(RdsUtils.isRdsDns(chinaCustomDomain));

    assertTrue(RdsUtils.isRdsDns(oldChinaCluster));
    assertTrue(RdsUtils.isRdsDns(oldChinaClusterReadOnly));
    assertTrue(RdsUtils.isRdsDns(oldChinaInstance));
    assertTrue(RdsUtils.isRdsDns(oldChinaProxy));
    assertTrue(RdsUtils.isRdsDns(oldChinaCustomDomain));
  }

  @Test
  public void testGetRdsInstanceHostPattern() {
    final String expectedHostPattern = "?.XYZ.us-east-2.rds.amazonaws.com";
    assertEquals(expectedHostPattern, RdsUtils.getRdsInstanceHostPattern(usEastCluster));
    assertEquals(expectedHostPattern, RdsUtils.getRdsInstanceHostPattern(usEastClusterReadOnly));
    assertEquals(expectedHostPattern, RdsUtils.getRdsInstanceHostPattern(usEastInstance));
    assertEquals(expectedHostPattern, RdsUtils.getRdsInstanceHostPattern(usEastProxy));
    assertEquals(expectedHostPattern, RdsUtils.getRdsInstanceHostPattern(usEastCustomDomain));

    final String chinaExpectedHostPattern = "?.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    assertEquals(chinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(chinaCluster));
    assertEquals(chinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(chinaClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(chinaInstance));
    assertEquals(chinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(chinaProxy));
    assertEquals(chinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(chinaCustomDomain));

    final String oldChinaExpectedHostPattern = "?.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(oldChinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(oldChinaCluster));
    assertEquals(oldChinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(
        oldChinaClusterReadOnly));
    assertEquals(oldChinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(oldChinaInstance));
    assertEquals(oldChinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(oldChinaProxy));
    assertEquals(oldChinaExpectedHostPattern, RdsUtils.getRdsInstanceHostPattern(
        oldChinaCustomDomain));
  }

  @Test
  public void testIsRdsClusterDns() {
    assertTrue(RdsUtils.isRdsClusterDns(usEastCluster));
    assertTrue(RdsUtils.isRdsClusterDns(usEastClusterReadOnly));
    assertFalse(RdsUtils.isRdsClusterDns(usEastInstance));
    assertFalse(RdsUtils.isRdsClusterDns(usEastProxy));
    assertFalse(RdsUtils.isRdsClusterDns(usEastCustomDomain));

    assertTrue(RdsUtils.isRdsClusterDns(chinaCluster));
    assertTrue(RdsUtils.isRdsClusterDns(chinaClusterReadOnly));
    assertFalse(RdsUtils.isRdsClusterDns(chinaInstance));
    assertFalse(RdsUtils.isRdsClusterDns(chinaProxy));
    assertFalse(RdsUtils.isRdsClusterDns(chinaCustomDomain));

    assertTrue(RdsUtils.isRdsClusterDns(oldChinaCluster));
    assertTrue(RdsUtils.isRdsClusterDns(oldChinaClusterReadOnly));
    assertFalse(RdsUtils.isRdsClusterDns(oldChinaInstance));
    assertFalse(RdsUtils.isRdsClusterDns(oldChinaProxy));
    assertFalse(RdsUtils.isRdsClusterDns(oldChinaCustomDomain));
  }

  @Test
  public void testIsReaderClusterDns() {
    assertFalse(RdsUtils.isReaderClusterDns(usEastCluster));
    assertTrue(RdsUtils.isReaderClusterDns(usEastClusterReadOnly));
    assertFalse(RdsUtils.isReaderClusterDns(usEastInstance));
    assertFalse(RdsUtils.isReaderClusterDns(usEastProxy));
    assertFalse(RdsUtils.isReaderClusterDns(usEastCustomDomain));

    assertFalse(RdsUtils.isReaderClusterDns(chinaCluster));
    assertTrue(RdsUtils.isReaderClusterDns(chinaClusterReadOnly));
    assertFalse(RdsUtils.isReaderClusterDns(chinaInstance));
    assertFalse(RdsUtils.isReaderClusterDns(chinaProxy));
    assertFalse(RdsUtils.isReaderClusterDns(chinaCustomDomain));

    assertFalse(RdsUtils.isReaderClusterDns(oldChinaCluster));
    assertTrue(RdsUtils.isReaderClusterDns(oldChinaClusterReadOnly));
    assertFalse(RdsUtils.isReaderClusterDns(oldChinaInstance));
    assertFalse(RdsUtils.isReaderClusterDns(oldChinaProxy));
    assertFalse(RdsUtils.isReaderClusterDns(oldChinaCustomDomain));
  }

  @Test
  public void testInvalidPathsHostPattern() {
    final String incorrectChinaHostPattern = "?.rds.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(incorrectChinaHostPattern, RdsUtils.getRdsInstanceHostPattern(extraRdsChinaPath));
    assertEquals("?", RdsUtils.getRdsInstanceHostPattern(missingCnChinaPath));
    assertEquals("?", RdsUtils.getRdsInstanceHostPattern(missingRegionChinaPath));
  }

  @Test
  public void testInvalidPathsReaderCluster() {
    assertFalse(RdsUtils.isReaderClusterDns(extraRdsChinaPath));
    assertFalse(RdsUtils.isReaderClusterDns(missingCnChinaPath));
    assertFalse(RdsUtils.isReaderClusterDns(missingRegionChinaPath));
  }

  @Test
  public void testInvalidPathsRdsDns() {
    // Expected to return true with correct cluster paths
    assertTrue(RdsUtils.isRdsDns(extraRdsChinaPath));
    assertFalse(RdsUtils.isRdsDns(missingCnChinaPath));
    assertFalse(RdsUtils.isRdsDns(missingRegionChinaPath));
  }
}