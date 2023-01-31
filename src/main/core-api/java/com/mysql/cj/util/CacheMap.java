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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class CacheMap<K,V> {

  private final Map<K, CacheItem<V>> cache = new ConcurrentHashMap<>();
  private final long cleanupIntervalNanos = TimeUnit.MINUTES.toNanos(10);
  private final AtomicLong cleanupTimeNanos = new AtomicLong(System.nanoTime() + cleanupIntervalNanos);

  public CacheMap() {
  }

  public V get(final K key) {
    CacheItem<V> cacheItem = cache.computeIfPresent(key, (kk, vv) -> vv.isExpired() ? null : vv);
    return cacheItem == null ? null : cacheItem.item;
  }

  public V get(final K key, final V defaultItemValue, long itemExpirationNano) {
    CacheItem<V> cacheItem = cache.compute(key,
        (kk, vv) -> (vv == null || vv.isExpired())
            ? new CacheItem<>(defaultItemValue, System.nanoTime() + itemExpirationNano)
            : vv);
    return cacheItem.item;
  }

  public void put(final K key, final V item, long itemExpirationNano) {
    cache.put(key, new CacheItem<>(item, System.nanoTime() + itemExpirationNano));
    cleanUp();
  }

  public void putIfAbsent(final K key, final V item, long itemExpirationNano) {
    cache.putIfAbsent(key, new CacheItem<>(item, System.nanoTime() + itemExpirationNano));
    cleanUp();
  }

  public void remove(final K key) {
    cache.remove(key);
    cleanUp();
  }

  public void clear() {
    cache.clear();
  }

  public int size() { return this.cache.size(); }

  private void cleanUp() {
    if (this.cleanupTimeNanos.get() < System.nanoTime()) {
      this.cleanupTimeNanos.set(System.nanoTime() + cleanupIntervalNanos);
      cache.entrySet().forEach(entry -> {
        if (entry.getValue() == null || entry.getValue().isExpired()) {
          cache.remove(entry.getKey());
        }
      });
    }
  }

  private static class CacheItem<V> {
    final V item;
    final long expirationTime;

    public CacheItem(V item, long expirationTime) {
      this.item = item;
      this.expirationTime = expirationTime;
    }

    boolean isExpired() {
      return System.nanoTime() > expirationTime;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((item == null) ? 0 : item.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      CacheItem<?> other = (CacheItem<?>) obj;
      if (item == null) {
        if (other.item != null) {
          return false;
        }
      } else if (!item.equals(other.item)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "CacheItem [item=" + item + ", expirationTime=" + expirationTime + "]";
    }
  }
}