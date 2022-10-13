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

import java.util.*;
import java.util.stream.Collectors;

public class ExpiringCache<K, V> implements Map<K, V> {

    private int expireTimeMs;

    private final LinkedHashMap<K, Hit<V>> linkedHashMap = new LinkedHashMap<K, Hit<V>>(1, 0.75F, true) {

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, Hit<V>> eldest) {
            if(eldest.getValue().isExpire(expireTimeMs)) {
                Iterator<Hit<V>> i = values().iterator();

                i.next();
                do {
                    i.remove();
                } while (i.hasNext() && i.next().isExpire(expireTimeMs));
            }
            return false;
        }
    };

    public ExpiringCache(int expireTimeMs) {
        this.expireTimeMs = expireTimeMs;
    }

    public void setExpireTime(int expireTimeMs) {
        this.expireTimeMs = expireTimeMs;
    }

    public int getExpireTime() {
        return this.expireTimeMs;
    }

    @Override
    public synchronized int size() {
        return (int)this.linkedHashMap.values().stream()
                .filter(x -> !x.isExpire(expireTimeMs))
                .count();
    }

    @Override
    public synchronized boolean isEmpty() {
        return !this.linkedHashMap.values().stream().anyMatch(x -> !x.isExpire(expireTimeMs));
    }

    @Override
    public synchronized boolean containsKey(Object key) {
        V payload = this.get(key);
        return payload != null;
    }

    @Override
    public synchronized boolean containsValue(Object value) {
        return this.linkedHashMap.values().stream().anyMatch(x -> !x.isExpire(expireTimeMs) && x.payload == value);
    }

    @Override
    public synchronized V get(Object key) {
        Hit<V> hit = this.linkedHashMap.get(key);

        if(hit == null) {
            return null;
        }

        if(hit.isExpire(expireTimeMs)) {
            this.linkedHashMap.remove(key);
            return null;
        }

        return hit.payload;
    }

    @Override
    public synchronized V put(K key, V value) {
        Hit<V> prevValue = this.linkedHashMap.put(key, new Hit<V>(value));
        return prevValue == null ? null : prevValue.payload;
    }

    @Override
    public synchronized V remove(Object key) {
        Hit<V> prevValue = this.linkedHashMap.remove(key);
        return prevValue == null ? null : prevValue.payload;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            this.linkedHashMap.put(entry.getKey(), new Hit<V>(entry.getValue()));
        }
    }

    @Override
    public synchronized void clear() {
        this.linkedHashMap.clear();
    }

    @Override
    public synchronized Set<K> keySet() {
        return this.linkedHashMap.entrySet().stream()
                .filter(x -> !x.getValue().isExpire(expireTimeMs))
                .map(x -> x.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    public synchronized Collection<V> values() {
        return this.linkedHashMap.values().stream()
                .filter(x -> !x.isExpire(expireTimeMs))
                .map(x -> x.payload)
                .collect(Collectors.toList());
    }

    @Override
    public synchronized Set<Entry<K, V>> entrySet() {
        return this.linkedHashMap.entrySet().stream()
                .filter(x -> !x.getValue().isExpire(expireTimeMs))
                .map(x -> new AbstractMap.SimpleEntry<K, V>(x.getKey(), x.getValue().payload))
                .collect(Collectors.toSet());
    }

    private class Hit<V> {
        private final long time;
        private final V payload;

        Hit(V payload) {
            this.time = System.currentTimeMillis();
            this.payload = payload;
        }

        boolean isExpire(int expireTimeMs) {
            return (System.currentTimeMillis() - this.time) >= expireTimeMs;
        }
    }
}
