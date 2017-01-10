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

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.test.InMemoryKeyValueStore;
import org.apache.kafka.test.KTableValueGetterStub;
import org.apache.kafka.test.MockKeyValueMapper;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.NoOpProcessorContext;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("unchecked")
public class KTableGlobalKTableJoinTest {
    private final InMemoryKeyValueStore<String, String> joinStore = new InMemoryKeyValueStore<>("joinStore");
    private final KTableValueGetterStub<String, String> global = new KTableValueGetterStub<>();
    private final KTableGlobalKTableJoin<String, String, String, String, String> join
            = new KTableGlobalKTableJoin<>(new ValueGetterSupplier<>(new KTableValueGetterStub<String, String>()),
                                           new ValueGetterSupplier<>(global),
                                           MockValueJoiner.STRING_JOINER,
                                           MockKeyValueMapper.<String, String>SelectValueMapper(),
                                           "joinStore");

    private NoOpProcessorContext context;

    @Before
    public void before() {
        context = new NoOpProcessorContext();
        context.register(joinStore, false, null);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotJoinIfNewValueDoesntMapToKeyInGlobalStore() throws Exception {
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<>("1", null));
        assertThat(joinStore.get("A"), is(nullValue()));
    }

    @Test
    public void shouldJoinAndPutValueInJoinStoreIfKeyFromNewValueInGlobalStore() throws Exception {
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        global.put("1", "B");
        processor.process("A", new Change<>("1", null));
        assertThat(joinStore.get("A"), equalTo("1+B"));
    }

    @Test
    public void shouldStoreDeleteIfChangeHasOldValue() throws Exception {
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("A", new Change<>("1", null));
        processor.process("A", new Change<>(null, "1"));
        assertThat(joinStore.get("A"), is(nullValue()));
    }


    @Test
    public void shouldNotJoinIfBothNewAndOldValuesAreNull() throws Exception {
        global.put("1", "A");
        final Processor<String, Change<String>> processor = join.get();
        processor.init(context);
        processor.process("1", new Change<String>(null, null));
        assertThat(joinStore.approximateNumEntries(), is(0L));
    }


    static class ValueGetterSupplier<K, V> implements KTableValueGetterSupplier<K, V> {

        private final KTableValueGetterStub<K, V> valueGetter;

        ValueGetterSupplier(final KTableValueGetterStub<K, V> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @Override
        public KTableValueGetter<K, V> get() {
            return valueGetter;
        }

        @Override
        public String[] storeNames() {
            return new String[0];
        }
    }

}