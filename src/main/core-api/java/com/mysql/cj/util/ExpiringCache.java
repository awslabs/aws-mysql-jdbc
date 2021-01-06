/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of this program hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of this connector, is also subject to the Universal FOSS Exception,
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
                } while(i.hasNext() && i.next().isExpire(expireTimeMs));
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
    public int size() {
        return (int)this.linkedHashMap.values().stream()
                .filter(x -> !x.isExpire(expireTimeMs))
                .count();
    }

    @Override
    public boolean isEmpty() {
        return !this.linkedHashMap.values().stream().anyMatch(x -> !x.isExpire(expireTimeMs));
    }

    @Override
    public boolean containsKey(Object key) {
        V payload = this.get(key);
        return payload != null;
    }

    @Override
    public boolean containsValue(Object value) {
        return this.linkedHashMap.values().stream().anyMatch(x -> !x.isExpire(expireTimeMs) && x.payload == value);
    }

    @Override
    public V get(Object key) {
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
    public V put(K key, V value) {
        Hit<V> prevValue = this.linkedHashMap.put(key, new Hit<V>(value));
        return prevValue == null ? null : prevValue.payload;
    }

    @Override
    public V remove(Object key) {
        Hit<V> prevValue = this.linkedHashMap.remove(key);
        return prevValue == null ? null : prevValue.payload;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            this.linkedHashMap.put(entry.getKey(), new Hit<V>(entry.getValue()));
        }
    }

    @Override
    public void clear() {
        this.linkedHashMap.clear();
    }

    @Override
    public Set<K> keySet() {
        return this.linkedHashMap.entrySet().stream()
                .filter(x -> !x.getValue().isExpire(expireTimeMs))
                .map(x -> x.getKey())
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return this.linkedHashMap.values().stream()
                .filter(x -> !x.isExpire(expireTimeMs))
                .map(x -> x.payload)
                .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
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
