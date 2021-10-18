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

package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

class MonitorConnectionContextTest {

  private static final String NODE = "any.node.domain";
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
        NODE,
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
    context.setConnectionValid(true, System.currentTimeMillis(), VALIDATION_INTERVAL_MILLIS);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(0, this.context.getFailureCount());
  }

  @Test
  public void test_2_isNodeUnhealthyWithInvalidConnection_returnFalse() {
    context.setConnectionValid(false, System.currentTimeMillis(), VALIDATION_INTERVAL_MILLIS);
    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(1, this.context.getFailureCount());
  }

  @Test
  public void test_3_isNodeUnhealthyExceedsFailureDetectionCount_returnTrue() {
    final int expectedFailureCount = FAILURE_DETECTION_COUNT + 1;
    context.setFailureCount(FAILURE_DETECTION_COUNT);
    context.resetInvalidNodeStartTime();

    context.setConnectionValid(false, System.currentTimeMillis(), VALIDATION_INTERVAL_MILLIS);

    Assertions.assertFalse(context.isNodeUnhealthy());
    Assertions.assertEquals(expectedFailureCount, context.getFailureCount());
    Assertions.assertTrue(context.isInvalidNodeStartTimeDefined());
  }

  @Test
  public void test_4_isNodeUnhealthyExceedsFailureDetectionCount_returnTrue() {
    long currentTimeMillis = System.currentTimeMillis();
    context.setFailureCount(0);
    context.resetInvalidNodeStartTime();

    // Simulate monitor loop that reports invalid connection for 6 times with interval 50 msec
    for(int i = 0; i < 6; i++) {
      context.setConnectionValid(false, currentTimeMillis, VALIDATION_INTERVAL_MILLIS);
      Assertions.assertFalse(context.isNodeUnhealthy());

      currentTimeMillis += VALIDATION_INTERVAL_MILLIS;
    }

    context.setConnectionValid(false, currentTimeMillis, VALIDATION_INTERVAL_MILLIS);
    Assertions.assertTrue(context.isNodeUnhealthy());
  }
}
