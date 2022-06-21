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

package com.mysql.cj.jdbc.ha.plugins;

import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class MonitorConnectionContextTest {

  private static final Set<String> NODE_KEYS = new HashSet<>(Collections.singletonList("any.node.domain"));
  private static final int FAILURE_DETECTION_TIME_MILLIS = 10;
  private static final int FAILURE_DETECTION_INTERVAL_MILLIS = 100;
  private static final int FAILURE_DETECTION_COUNT = 3;
  private static final int VALIDATION_INTERVAL_MILLIS = 50;

  private MonitorConnectionContext context;
  private AutoCloseable closeable;

  @BeforeEach
  void init() {
    closeable = MockitoAnnotations.openMocks(this);
    context = new MonitorConnectionContext(
        null,
        NODE_KEYS,
        new NullLogger(MonitorConnectionContextTest.class.getName()),
        FAILURE_DETECTION_TIME_MILLIS,
        FAILURE_DETECTION_INTERVAL_MILLIS,
        FAILURE_DETECTION_COUNT);
  }

  @AfterEach
  void cleanUp() throws Exception {
    closeable.close();
  }

  @Test
  public void test_1_isNodeUnhealthyWithConnection_returnFalse() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setConnectionValid(true, currentTimeMillis, currentTimeMillis);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(0, this.context.getFailureCount());
  }

  @Test
  public void test_2_isNodeUnhealthyWithInvalidConnection_returnFalse() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setConnectionValid(false, currentTimeMillis, currentTimeMillis);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(1, this.context.getFailureCount());
  }

  @Test
  public void test_3_isNodeUnhealthyExceedsFailureDetectionCount_returnTrue() {
    final int expectedFailureCount = FAILURE_DETECTION_COUNT + 1;
    context.setFailureCount(FAILURE_DETECTION_COUNT);
    context.resetInvalidNodeStartTime();

    long currentTimeMillis = System.currentTimeMillis();
    context.setConnectionValid(false, currentTimeMillis, currentTimeMillis);

    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(expectedFailureCount, context.getFailureCount());
    Assertions.assertTrue(context.isInvalidNodeStartTimeDefined());
  }

  @Test
  public void test_4_isNodeUnhealthyExceedsFailureDetectionCount() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setFailureCount(0);
    context.resetInvalidNodeStartTime();

    // Simulate monitor loop that reports invalid connection for 5 times with interval 50 msec to wait 250 msec in total
    for (int i = 0; i < 5; i++) {
      long statusCheckStartTime = currentTimeMillis;
      long statusCheckEndTime = currentTimeMillis + VALIDATION_INTERVAL_MILLIS;

      context.setConnectionValid(false, statusCheckStartTime, statusCheckEndTime);
      Assertions.assertFalse(context.isNodeUnhealthy());

      currentTimeMillis += VALIDATION_INTERVAL_MILLIS;
    }

    // Simulate waiting another 50 msec that makes total waiting time to 300 msec
    // Expected max waiting time for this context is 300 msec (interval 100 msec, count 3)
    // So it's expected that this run turns node status to "unhealthy" since we reached max allowed waiting time.

    long statusCheckStartTime = currentTimeMillis;
    long statusCheckEndTime = currentTimeMillis + VALIDATION_INTERVAL_MILLIS;

    context.setConnectionValid(false, statusCheckStartTime, statusCheckEndTime);
    Assertions.assertTrue(context.isNodeUnhealthy());
  }
}
