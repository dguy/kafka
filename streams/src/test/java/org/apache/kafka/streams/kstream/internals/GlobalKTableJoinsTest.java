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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;


public class GlobalKTableJoinsTest {

    private final KStreamBuilder builder = new KStreamBuilder();
    private GlobalKTable<String, String> global;
    private File stateDir;
    private final Map<String, String> results = new HashMap<>();
    private KStream<String, String> stream;
    private KeyValueMapper<String, String, String> keyValueMapper;
    private ForeachAction<String, String> action;
    private KTable<String, String> table;
    private final String streamTopic = "stream";
    private final String globalTopic = "global";
    private final String tableTopic = "table";

    @Before
    public void setUp() throws Exception {
        stateDir = TestUtils.tempDirectory();
        global = builder.globalTable(Serdes.String(), Serdes.String(), globalTopic, "global-store");
        stream = builder.stream(Serdes.String(), Serdes.String(), streamTopic);
        table = builder.table(Serdes.String(), Serdes.String(), tableTopic, tableTopic);

        keyValueMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(final String key, final String value) {
                return value;
            }
        };
        action = new ForeachAction<String, String>() {
            @Override
            public void apply(final String key, final String value) {
                results.put(key, value);
            }
        };

    }

    @Test
    public void shouldLeftJoinWithStream() throws Exception {
        stream.leftJoin(global, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");
        expected.put("3", "c+null");

        verifyJoin(expected, streamTopic);

    }

    @Test
    public void shouldInnerJoinWithStream() throws Exception {
        stream.join(global, keyValueMapper, MockValueJoiner.STRING_JOINER)
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");

        verifyJoin(expected, streamTopic);
    }

    @Test
    public void shouldLeftJoinWithTable() throws Exception {
        table.leftJoin(global, keyValueMapper, MockValueJoiner.STRING_JOINER, Serdes.String(), Serdes.String())
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");
        expected.put("3", "c+null");

        verifyJoin(expected, tableTopic);
    }


    @Test
    public void shouldJoinWithTable() throws Exception {
        table.join(global,
                   keyValueMapper,
                   MockValueJoiner.STRING_JOINER,
                   Serdes.String(),
                   Serdes.String())
                .foreach(action);

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "a+A");
        expected.put("2", "b+B");

        verifyJoin(expected, tableTopic);
    }

    @Test
    public void shouldLeftJoinWithKTableAndAggregateCorrectly() throws Exception {
        final GlobalKTable<String, String> g1 = builder.globalTable(Serdes.String(), Serdes.String(), "foo", "foo");
        final KTable<Integer, String> t1 = builder.table(Serdes.Integer(), Serdes.String(), "t1", "t1");
        final Map<String, Long> countResults = new HashMap<>();
        t1.leftJoin(g1, new KeyValueMapper<Integer, String, String>() {
            @Override
            public String apply(final Integer key, final String value) {
                return value;
            }
        }, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value2;
            }
        }, Serdes.Integer(), Serdes.String()).groupBy(new KeyValueMapper<Integer, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(final Integer key, final String value) {
                return KeyValue.pair(value, value);
            }
        }, Serdes.String(), Serdes.String()).count("count").foreach(new ForeachAction<String, Long>() {
            @Override
            public void apply(final String key, final Long value) {
                countResults.put(key, value);
            }
        });

        runAndVerifyKTableJoinThenAggregate(countResults);
    }

    @Test
    public void shouldJoinWithKTableAndAggregateCorrectly() throws Exception {
        final GlobalKTable<String, String> g1 = builder.globalTable(Serdes.String(), Serdes.String(), "foo", "foo");
        final KTable<Integer, String> t1 = builder.table(Serdes.Integer(), Serdes.String(), "t1", "t1");
        final Map<String, Long> countResults = new HashMap<>();
        t1.join(g1, new KeyValueMapper<Integer, String, String>() {
            @Override
            public String apply(final Integer key, final String value) {
                return value;
            }
        }, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(final String value1, final String value2) {
                return value2;
            }
        }, Serdes.Integer(), Serdes.String()).groupBy(new KeyValueMapper<Integer, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(final Integer key, final String value) {
                return KeyValue.pair(value, value);
            }
        }, Serdes.String(), Serdes.String()).count("count").foreach(new ForeachAction<String, Long>() {
            @Override
            public void apply(final String key, final Long value) {
                countResults.put(key, value);
            }
        });

        runAndVerifyKTableJoinThenAggregate(countResults);
    }

    private void runAndVerifyKTableJoinThenAggregate(final Map<String, Long> countResults) {
        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0L);
        driver.process("foo", "1", "green");
        driver.process("foo", "2", "blue");
        driver.process("foo", "3", "yellow");
        driver.process("foo", "4", "red");
        driver.process("t1", 1, "1");
        driver.process("t1", 2, "1");
        driver.process("t1", 3, "1");
        driver.flushState();

        assertThat(countResults, equalTo(Collections.singletonMap("green", 3L)));

        driver.process("foo", "1", "orange");
        driver.process("t1", 1, "4");
        driver.flushState();

        final Map<String, Long> expected = new HashMap<>();
        expected.put("green", 2L);
        expected.put("red", 1L);

        assertThat(countResults, equalTo(expected));
    }


    private void verifyJoin(final Map<String, String> expected, final String joinInput) {
        final KStreamTestDriver driver = new KStreamTestDriver(builder, stateDir);
        driver.setTime(0L);
        // write some data to the global table
        driver.process(globalTopic, "a", "A");
        driver.process(globalTopic, "b", "B");
        //write some data to the stream
        driver.process(joinInput, "1", "a");
        driver.process(joinInput, "2", "b");
        driver.process(joinInput, "3", "c");
        driver.flushState();

        assertEquals(expected, results);
    }
}
