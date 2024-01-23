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

package com.mysql.cj.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mysql.cj.jdbc.ha.util.RdsUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RdsUtilsTests {

  private RdsUtils target;
  private static final String usEastRegionCluster =
      "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionInstance =
      "instance-test-name.XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionProxy =
      "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com";
  private static final String usEastRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com";

  private static final String chinaRegionCluster =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionInstance =
      "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionProxy =
      "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn";
  private static final String chinaRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn";

  private static final String oldChinaRegionCluster =
      "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionClusterReadOnly =
      "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionInstance =
      "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionProxy =
      "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String oldChinaRegionCustomDomain =
      "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn";

  private static final String extraRdsChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn";
  private static final String missingCnChinaPath =
      "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com";
  private static final String missingRegionChinaPath =
      "database-test-name.cluster-XYZ.rds.amazonaws.com.cn";

  @BeforeEach
  public void setupTests() {
    target = new RdsUtils();
  }

  @Test
  public void testIsRdsDns() {
    assertTrue(target.isRdsDns(usEastRegionCluster));
    assertTrue(target.isRdsDns(usEastRegionClusterReadOnly));
    assertTrue(target.isRdsDns(usEastRegionInstance));
    assertTrue(target.isRdsDns(usEastRegionProxy));
    assertTrue(target.isRdsDns(usEastRegionCustomDomain));

    assertTrue(target.isRdsDns(chinaRegionCluster));
    assertTrue(target.isRdsDns(chinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(chinaRegionInstance));
    assertTrue(target.isRdsDns(chinaRegionProxy));
    assertTrue(target.isRdsDns(chinaRegionCustomDomain));

    assertTrue(target.isRdsDns(oldChinaRegionCluster));
    assertTrue(target.isRdsDns(oldChinaRegionClusterReadOnly));
    assertTrue(target.isRdsDns(oldChinaRegionInstance));
    assertTrue(target.isRdsDns(oldChinaRegionProxy));
    assertTrue(target.isRdsDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testGetRdsInstanceHostPattern() {
    final String expectedHostPattern = "?.XYZ.us-east-2.rds.amazonaws.com";
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionCluster));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionClusterReadOnly));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionInstance));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionProxy));
    assertEquals(expectedHostPattern, target.getRdsInstanceHostPattern(usEastRegionCustomDomain));

    final String chinaExpectedHostPattern = "?.XYZ.rds.cn-northwest-1.amazonaws.com.cn";
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionCluster));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionClusterReadOnly));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionInstance));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionProxy));
    assertEquals(chinaExpectedHostPattern, target.getRdsInstanceHostPattern(chinaRegionCustomDomain));

    final String oldChinaExpectedHostPattern = "?.XYZ.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionCluster));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionClusterReadOnly));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionInstance));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionProxy));
    assertEquals(oldChinaExpectedHostPattern, target.getRdsInstanceHostPattern(oldChinaRegionCustomDomain));
  }

  @Test
  public void testIsRdsClusterDns() {
    assertTrue(target.isRdsClusterDns(usEastRegionCluster));
    assertTrue(target.isRdsClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(usEastRegionInstance));
    assertFalse(target.isRdsClusterDns(usEastRegionProxy));
    assertFalse(target.isRdsClusterDns(usEastRegionCustomDomain));

    assertTrue(target.isRdsClusterDns(chinaRegionCluster));
    assertTrue(target.isRdsClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(chinaRegionInstance));
    assertFalse(target.isRdsClusterDns(chinaRegionProxy));
    assertFalse(target.isRdsClusterDns(chinaRegionCustomDomain));

    assertTrue(target.isRdsClusterDns(oldChinaRegionCluster));
    assertTrue(target.isRdsClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isRdsClusterDns(oldChinaRegionInstance));
    assertFalse(target.isRdsClusterDns(oldChinaRegionProxy));
    assertFalse(target.isRdsClusterDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testIsReaderClusterDns() {
    assertFalse(target.isReaderClusterDns(usEastRegionCluster));
    assertTrue(target.isReaderClusterDns(usEastRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(usEastRegionInstance));
    assertFalse(target.isReaderClusterDns(usEastRegionProxy));
    assertFalse(target.isReaderClusterDns(usEastRegionCustomDomain));

    assertFalse(target.isReaderClusterDns(chinaRegionCluster));
    assertTrue(target.isReaderClusterDns(chinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(chinaRegionInstance));
    assertFalse(target.isReaderClusterDns(chinaRegionProxy));
    assertFalse(target.isReaderClusterDns(chinaRegionCustomDomain));

    assertFalse(target.isReaderClusterDns(oldChinaRegionCluster));
    assertTrue(target.isReaderClusterDns(oldChinaRegionClusterReadOnly));
    assertFalse(target.isReaderClusterDns(oldChinaRegionInstance));
    assertFalse(target.isReaderClusterDns(oldChinaRegionProxy));
    assertFalse(target.isReaderClusterDns(oldChinaRegionCustomDomain));
  }

  @Test
  public void testBrokenPathsHostPattern() {
    final String incorrectChinaHostPattern = "?.rds.cn-northwest-1.rds.amazonaws.com.cn";
    assertEquals(incorrectChinaHostPattern, target.getRdsInstanceHostPattern(extraRdsChinaPath));
    assertEquals("?", target.getRdsInstanceHostPattern(missingCnChinaPath));
    assertEquals("?", target.getRdsInstanceHostPattern(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsReaderCluster() {
    assertFalse(target.isReaderClusterDns(extraRdsChinaPath));
    assertFalse(target.isReaderClusterDns(missingCnChinaPath));
    assertFalse(target.isReaderClusterDns(missingRegionChinaPath));
  }

  @Test
  public void testBrokenPathsRdsDns() {
    // Expected to return true with correct cluster paths
    assertTrue(target.isRdsDns(extraRdsChinaPath));
    assertFalse(target.isRdsDns(missingCnChinaPath));
    assertFalse(target.isRdsDns(missingRegionChinaPath));
  }
}