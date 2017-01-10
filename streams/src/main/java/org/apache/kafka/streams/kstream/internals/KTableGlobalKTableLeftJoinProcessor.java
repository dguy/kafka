/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;

class KTableGlobalKTableLeftJoinProcessor<K, GK, V, GV, RV> extends AbstractProcessor<K, Change<V>> {

    private final KTableValueGetter<GK, GV> valueGetter;
    private final ValueJoiner<V, GV, RV> joiner;
    private final KeyValueMapper<K, V, GK> keyValueMapper;
    private final boolean sendOldValues;
    private final String joinResultStoreName;
    private KeyValueStore<K, RV> joinResultStore;

    KTableGlobalKTableLeftJoinProcessor(final KTableValueGetter<GK, GV> valueGetter,
                                        final ValueJoiner<V, GV, RV> joiner,
                                        final KeyValueMapper<K, V, GK> keyValueMapper,
                                        final boolean sendOldValues,
                                        final String joinResultStoreName) {
        this.valueGetter = valueGetter;
        this.joiner = joiner;
        this.keyValueMapper = keyValueMapper;
        this.sendOldValues = sendOldValues;
        this.joinResultStoreName = joinResultStoreName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        valueGetter.init(context);
        joinResultStore = (KeyValueStore<K, RV>) context.getStateStore(joinResultStoreName);
        ((CachedStateStore) joinResultStore).setFlushListener(new ForwardingCacheFlushListener<K, V>(context, sendOldValues));
    }

    /**
     * @throws StreamsException if key is null
     */
    @Override
    public void process(final K key, final Change<V> change) {
        // the keys should never be null
        if (key == null) {
            throw new StreamsException("Record key for KTable left-join operator should not be null.");
        }

        if (change.newValue == null && change.oldValue == null) {
            return;
        }

        // if new value is null we need to send (key, null) downstream
        if (change.newValue == null) {
            joinResultStore.put(key, null);
            return;
        }

        final GV newGlobalValue = valueGetter.get(keyValueMapper.apply(key, change.newValue));
        final RV result = joiner.apply(change.newValue, newGlobalValue);
        joinResultStore.put(key, result);
    }

}
