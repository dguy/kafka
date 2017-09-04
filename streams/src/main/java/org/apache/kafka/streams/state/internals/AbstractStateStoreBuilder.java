/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StateStoreBuilder;

import java.util.Map;
import java.util.Objects;

abstract class AbstractStateStoreBuilder<K, V, T extends StateStore> implements StateStoreBuilder<T> {
    private final String name;
    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    final Time time;
    Map<String, String> logConfig;
    boolean enableCaching;
    boolean enableLogging;

    public AbstractStateStoreBuilder(final String name,
                                     final Serde<K> keySerde,
                                     final Serde<V> valueSerde,
                                     final Time time) {
        Objects.requireNonNull(name, "name can't be null");
        Objects.requireNonNull(time, "time can't be null");
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.time = time;
    }

    public StateStoreBuilder<T> withCachingEnabled() {
        enableCaching = true;
        return this;
    }

    public StateStoreBuilder<T> withLoggingEnabled(final Map<String, String> config) {
        enableLogging = true;
        logConfig = config;
        return this;
    }

    public Map<String, String> logConfig() {
        return logConfig;
    }

    public boolean loggingEnabled() {
        return enableLogging;
    }

    @Override
    public String name() {
        return name;
    }
}
