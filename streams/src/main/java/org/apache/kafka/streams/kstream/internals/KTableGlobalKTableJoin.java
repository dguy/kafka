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
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.CachedStateStore;

class KTableGlobalKTableJoin<K, GK, R, V, GV> implements KTableProcessorSupplier<K, V, R> {

    private final KTableValueGetterSupplier<K, V> valueGetterSupplier;
    private final KTableValueGetterSupplier<GK, GV> globalTableValueGetterSupplier;
    private final ValueJoiner<V, GV, R> joiner;
    private final KeyValueMapper<K, V, GK> mapper;
    private final String joinResultStoreName;
    private boolean sendOldValues;

    KTableGlobalKTableJoin(final KTableValueGetterSupplier<K, V> tableValueGetterSupplier,
                           final KTableValueGetterSupplier<GK, GV> globalTableValueGetterSupplier,
                           final ValueJoiner<V, GV, R> joiner,
                           final KeyValueMapper<K, V, GK> mapper,
                           final String joinResultStoreName) {
        this.valueGetterSupplier = tableValueGetterSupplier;
        this.globalTableValueGetterSupplier = globalTableValueGetterSupplier;
        this.joiner = joiner;
        this.mapper = mapper;
        this.joinResultStoreName = joinResultStoreName;
    }

    @Override
    public Processor<K, Change<V>> get() {
        return new KTableGlobalKTableJoinProcessor(globalTableValueGetterSupplier.get());
    }

    @Override
    public KTableValueGetterSupplier<K, R> view() {
        return new KTableGlobalKTableJoinGetterSupplier();
    }

    @Override
    public void enableSendingOldValues() {
        sendOldValues = true;
    }

    private class KTableGlobalKTableJoinGetterSupplier implements KTableValueGetterSupplier<K, R> {

        @Override
        public KTableValueGetter<K, R> get() {
            return new KTableKTableJoinValueGetter<>(valueGetterSupplier.get(),
                                                     globalTableValueGetterSupplier.get(),
                                                     joiner,
                                                     mapper);
        }

        @Override
        public String[] storeNames() {
            return valueGetterSupplier.storeNames();
        }
    }


    private class KTableGlobalKTableJoinProcessor extends AbstractProcessor<K, Change<V>> {

        private final KTableValueGetter<GK, GV> valueGetter;
        private KeyValueStore<K, R> joinResultStore;

        KTableGlobalKTableJoinProcessor(final KTableValueGetter<GK, GV> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
            joinResultStore = (KeyValueStore<K, R>) context.getStateStore(joinResultStoreName);
            ((CachedStateStore) joinResultStore).setFlushListener(new ForwardingCacheFlushListener<K, V>(context, sendOldValues));
        }

        /**
         * @throws StreamsException if key is null
         */
        @Override
        public void process(final K key, final Change<V> change) {
            // the keys should never be null
            if (key == null) {
                throw new StreamsException("Record key for KTable join operator should not be null.");
            }

            if (change.newValue == null && change.oldValue == null) {
                return;
            }

            if (change.newValue == null) {
                joinResultStore.put(key, null);
                return;
            }

            final GV newOtherValue = valueGetter.get(mapper.apply(key, change.newValue));

            if (newOtherValue != null) {
                final R result = joiner.apply(change.newValue, newOtherValue);
                joinResultStore.put(key, result);
            }

        }

    }


}
