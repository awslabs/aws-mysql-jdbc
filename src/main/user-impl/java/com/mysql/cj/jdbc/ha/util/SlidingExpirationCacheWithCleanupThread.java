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

package com.mysql.cj.jdbc.ha.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class SlidingExpirationCacheWithCleanupThread<K, V> extends SlidingExpirationCache<K, V> {

  private static final Logger LOGGER =
      Logger.getLogger(SlidingExpirationCacheWithCleanupThread.class.getName());

  protected static final ExecutorService cleanupThreadPool = Executors.newFixedThreadPool(1, runnableTarget -> {
    final Thread monitoringThread = new Thread(runnableTarget);
    monitoringThread.setDaemon(true);
    return monitoringThread;
  });

  public SlidingExpirationCacheWithCleanupThread() {
    super();
    this.initCleanupThread();
  }

  public SlidingExpirationCacheWithCleanupThread(
      final ShouldDisposeFunc<V> shouldDisposeFunc,
      final ItemDisposalFunc<V> itemDisposalFunc) {
    super(shouldDisposeFunc, itemDisposalFunc);
    this.initCleanupThread();
  }

  public SlidingExpirationCacheWithCleanupThread(
      final ShouldDisposeFunc<V> shouldDisposeFunc,
      final ItemDisposalFunc<V> itemDisposalFunc,
      final long cleanupIntervalNanos) {
    super(shouldDisposeFunc, itemDisposalFunc, cleanupIntervalNanos);
    this.initCleanupThread();
  }

  protected void initCleanupThread() {
    cleanupThreadPool.submit(() -> {
      while (true) {
        TimeUnit.NANOSECONDS.sleep(this.cleanupIntervalNanos);

        LOGGER.finest("Cleaning up...");
        this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
        cache.forEach((key, value) -> {
          try {
            removeIfExpired(key);
          } catch (Exception ex) {
            // ignore
          }
        });
      }
    });
    cleanupThreadPool.shutdown();
  }

  @Override
  protected void cleanUp() {
    // Intentionally do nothing. Cleanup thread does the job.
  }
}
