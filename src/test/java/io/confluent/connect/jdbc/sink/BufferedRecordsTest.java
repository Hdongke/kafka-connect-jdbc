/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.dialect.GreenplumDatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BufferedRecordsTest {

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

  @Before
  public void setUp() throws IOException, SQLException {
    sqliteHelper.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    sqliteHelper.tearDown();
  }
  
  
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSchemaExpend() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema schemaInner = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("ts", Schema.INT32_SCHEMA)
        .field("age",Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    final Schema schemaA = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", schemaInner)
        .build();

    final Struct valueInner = new Struct(schemaInner)
        .put("id", 1)
        .put("ts", 100)
        .put("age", 20)
        .put("name", "nick");

    final Struct valueA = new Struct(schemaA)
        .put("updated", "123")
        .put("after", valueInner);

    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    SinkRecord recordExpended = buffer.expand(recordA);

    final Schema schemaB = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("ts", Schema.INT32_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    final Struct valueB = new Struct(schemaB)
        .put("id", 1)
        .put("ts", 100)
        .put("age", 20)
        .put("name", "nick");

    final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
        assert(recordB.equals(recordExpended));
    
    final SinkRecord recordC = buffer.expand(recordB);
        assert(recordC.equals(recordB));
  }

  @Test
  public void testVoidExpend() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema schemaInner = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("ts", Schema.INT32_SCHEMA)
        .field("age",Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    final Schema schemaA = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", schemaInner)
        .build();

    thrown.expect(DataException.class);
    thrown.expectMessage("Invalid value: null used for required field: \"age\", schema type: INT32");

    final Struct valueInner = new Struct(schemaInner)
        .put("id", 1)
        .put("ts", 100)
        .put("age", null)
        .put("name", "nick");

    thrown.expect(DataException.class);
    thrown.expectMessage("Invalid value: null used for required field: \"age\", schema type: INT32");

    final Struct valueA = new Struct(schemaA)
        .put("updated", "123")
        .put("after", valueInner);

    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    SinkRecord recordExpended = buffer.expand(recordA);
  }

  
  @Test
  public void testEmptyAfterExpend() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema schemaA = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", Schema.STRING_SCHEMA)
        .build();

    thrown.expect(DataException.class);
    thrown.expectMessage("Cannot list fields on non-struct type");

    final Struct valueA = new Struct(schemaA)
        .put("updated", "")
        .put("after", "");

    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    SinkRecord recordExpended = buffer.expand(recordA);
  }

  @Test
  public void testChangeValueOfUpdatedAndAfterExpend() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema schemaA = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", Schema.BOOLEAN_SCHEMA)
        .build();

    thrown.expect(DataException.class);
    thrown.expectMessage("Invalid Java object for schema type STRING: class java.lang.Boolean for field: \"updated\"");

    final Struct valueA = new Struct(schemaA)
        .put("updated", true)
        .put("after", "123");

    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    SinkRecord recordExpended = buffer.expand(recordA);
  }

  @Test
  public void testExpend() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    /*
    the struct of schemaC is like this.
    schema ++++++++ updated
                       +
                    after ++++++++ name
                                    +
                                    age
                                    +
                                    other1 ++++++++ id
                                    +               +
                                    +               ts
                                    +
                                    other2 ++++++++ n1
                                                    +
                                                    n2
    we want to the struct of schemaC be like this.
    schama{updated: " " , name: " " , age: " " ; id: " " , ts: " " , n1: " " , n2 : " " } 
    */
    final Schema schemaInner2 = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInner3 = SchemaBuilder.struct()
    .field("n1",Schema.INT32_SCHEMA)
    .field("n2",Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInner1 = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("other1", schemaInner2)
    .field("other2", schemaInner3)
    .build();

    final Schema schema = SchemaBuilder.struct()
    .field("updated", Schema.STRING_SCHEMA)
    .field("after", schemaInner1)
    .build();

    final Struct value2 = new Struct(schemaInner2)
    .put("id", 1)
    .put("ts", 2);

    final Struct value3 = new Struct(schemaInner3)
    .put("n1", 3)
    .put("n2", 4);

    final Struct value1 = new Struct(schemaInner1)
    .put("name", "he")
    .put("age", 23)
    .put("other1", value2)
    .put("other2", value3);

    final Struct value = new Struct(schema)
    .put("updated", "123")
    .put("after", value1);

    final SinkRecord record = new SinkRecord("dummy", 0, null, null, schema, value, 0);
    SinkRecord recordExpended = buffer.expand(record);

    final Schema schemaResult = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .field("n1", Schema.INT32_SCHEMA)
    .field("n2", Schema.INT32_SCHEMA)
    .build();

    final Struct valueResult = new Struct(schemaResult)
    .put("name", "he")
    .put("age", 23)
    .put("id", 1)
    .put("ts", 2)
    .put("n1", 3)
    .put("n2", 4);

    final SinkRecord recordResult = new SinkRecord("dummy", 0, null, null, schemaResult, valueResult, 0);
    assert(recordResult.equals(recordExpended));

    final SinkRecord recordRes = buffer.expand(recordResult);
    assert(recordRes.equals(recordResult));
  }

  @Test
  public void testMultilevelExpend() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    /*
    the struct of schemaC is like this.
    schema ++++ {t_avro_envelope:STRUCT}
    |
    +--Field {name=updated, schema=STRING}
    +--Field {name=after, schema={t_avro:STRUCT}}
                    |
                    +--Field{name=name, schema=STRING}
                    +--Field{name=age, schema=INT32}
                    +--Field{name=other1, schema={t_avro:STRUCT}}
                    |                |
                    |               +--Field{name=id, schema=INT32}
                    |               +--Field{name=ts, schema=INT32}
                    |               +--Field{name=more1, schema={t_avro:STRUCT}}
                    |                            |
                    |                           +--Field{name=more1, schema={t_avro:STRUCT}}
                    |                                        |
                    |                                       +--Field{name=m1, schema=INT32}
                    |                                       +--Field{name=m2, schema=INT32}
                    |
                    +--Field{name=other2, schema={t_avro:STRUCT}}
                                |
                                +--Field{name=n1, schema=INT32}
                                +--Field{name=n2, schema=INT32}
                                +--Field{name=more2, schema={t_avro:STRUCT}}
                                            |
                                            +--Field{name=m3, schema=INT32}
                                            +--Field{name=m4, schema=INT32}
    we want to the struct of schemaC be like this.
    schama{updated: " " , name: " " , age: " " ; id: " " , ts: " " , m1: " ", m2: " ", n1: " " , n2 : " ", m3: " "; m4: ""} 
    */
    final Schema schemaInnerA1 = SchemaBuilder.struct()
    .field("m1", Schema.INT32_SCHEMA)
    .field("m2", Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInnerA2 = SchemaBuilder.struct()
    .field("m3", Schema.INT32_SCHEMA)
    .field("m4", Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInnerB1 = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .field("more1", schemaInnerA1)
    .build();

    final Schema schemaInnerB2 = SchemaBuilder.struct()
    .field("n1",Schema.INT32_SCHEMA)
    .field("n2",Schema.INT32_SCHEMA)
    .field("more2", schemaInnerA2)
    .build();

    final Schema schemaInnerC = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("other1", schemaInnerB1)
    .field("other2", schemaInnerB2)
    .build();

    final Schema schema = SchemaBuilder.struct()
    .field("updated", Schema.STRING_SCHEMA)
    .field("after", schemaInnerC)
    .build();

    final Struct valueA1 = new Struct(schemaInnerA1)
    .put("m1", 1)
    .put("m2", 2);

    final Struct valueA2 = new Struct(schemaInnerA2)
    .put("m3", 3)
    .put("m4", 4);

    final Struct valueB1 = new Struct(schemaInnerB1)
    .put("id", 1)
    .put("ts", 2)
    .put("more1", valueA1);

    final Struct valueB2 = new Struct(schemaInnerB2)
    .put("n1", 3)
    .put("n2", 4)
    .put("more2", valueA2);

    final Struct valueC = new Struct(schemaInnerC)
    .put("name", "he")
    .put("age", 23)
    .put("other1", valueB1)
    .put("other2", valueB2);

    final Struct value = new Struct(schema)
    .put("updated", "123")
    .put("after", valueC);

    final SinkRecord record = new SinkRecord("dummy", 0, null, null, schema, value, 0);
    SinkRecord recordExpended = buffer.expand(record);

    final Schema schemaResult = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .field("m1", Schema.INT32_SCHEMA)
    .field("m2", Schema.INT32_SCHEMA)
    .field("n1", Schema.INT32_SCHEMA)
    .field("n2", Schema.INT32_SCHEMA)
    .field("m3", Schema.INT32_SCHEMA)
    .field("m4", Schema.INT32_SCHEMA)
    .build();

    final Struct valueResult = new Struct(schemaResult)
    .put("name", "he")
    .put("age", 23)
    .put("id", 1)
    .put("ts", 2)
    .put("m1", 1)
    .put("m2", 2)
    .put("n1", 3)
    .put("n2", 4)
    .put("m3", 3)
    .put("m4", 4);

    final SinkRecord recordResult = new SinkRecord("dummy", 0, null, null, schemaResult, valueResult, 0);
    assert(recordResult.equals(recordExpended));

    final SinkRecord recordRes = buffer.expand(recordResult);
    assert(recordRes.equals(recordResult));
  }

  @Test
    public void testSameFieldExpend() {
        final HashMap<Object, Object> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", true);
        props.put("auto.evolve", true);
        props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
        final JdbcSinkConfig config = new JdbcSinkConfig(props);
    
        final String url = sqliteHelper.sqliteUri();
        final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
        final DbStructure dbStructure = new DbStructure(dbDialect);
    
        final TableId tableId = new TableId(null, null, "dummy");
        final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);
    
        /*
        the struct of schemaC is like this.
        schema ++++++++ updated
                        +
                        after ++++++++ name
                                        +
                                        age
                                        +
                                        other1 ++++++++ id
                                        +               +
                                        +               ts
                                        +
                                        other2 ++++++++ id
                                                        +
                                                        ts
        we want to the struct of schemaC be like this.
        schama{updated: " " , name: " " , age: " " ; id: " " , ts: " " , n1: " " , n2 : " " } 
        */
        final Schema schemaInner2 = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("ts", Schema.INT32_SCHEMA)
        .build();
    
        final Schema schemaInner3 = SchemaBuilder.struct()
        .field("id",Schema.INT32_SCHEMA)
        .field("ts",Schema.INT32_SCHEMA)
        .build();
    
        final Schema schemaInner1 = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("other1", schemaInner2)
        .field("other2", schemaInner3)
        .build();
    
        final Schema schema = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", schemaInner1)
        .build();
    
        final Struct value2 = new Struct(schemaInner2)
        .put("id", 1)
        .put("ts", 2);
    
        final Struct value3 = new Struct(schemaInner3)
        .put("id", 3)
        .put("ts", 4);
    
        final Struct value1 = new Struct(schemaInner1)
        .put("name", "he")
        .put("age", 23)
        .put("other1", value2)
        .put("other2", value3);
    
        final Struct value = new Struct(schema)
        .put("updated", "123")
        .put("after", value1);
    
        final SinkRecord record = new SinkRecord("dummy", 0, null, null, schema, value, 0);
        thrown.expect(SchemaBuilderException.class);
        thrown.expectMessage("Cannot create field because of field name duplication id");
        SinkRecord recordExpended = buffer.expand(record);

        /*
        the struct of schemaC is like this.
        schema ++++++++ updated
                        +
                        after ++++++++ name
                                        +
                                        age
                                        +
                                        other1 ++++++++ id
                                        +               +
                                        +               age
                                        +
                                        other2 ++++++++ n1
                                                        +
                                                        n2
        we want to the struct of schemaC be like this.
        schama{updated: " " , name: " " , age: " " ; id: " " , ts: " " , n1: " " , n2 : " " } 
        */

        final Schema schemaInnerB2 = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .build();
    
        final Schema schemaInnerB3 = SchemaBuilder.struct()
        .field("n1",Schema.INT32_SCHEMA)
        .field("n2",Schema.INT32_SCHEMA)
        .build();
    
        final Schema schemaInnerB1 = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("other1", schemaInnerB2)
        .field("other2", schemaInnerB3)
        .build();
    
        final Schema schemaB = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", schemaInnerB1)
        .build();
    
        final Struct valueB2 = new Struct(schemaInnerB2)
        .put("id", 1)
        .put("age", 2);
    
        final Struct valueB3 = new Struct(schemaInnerB3)
        .put("n1", 3)
        .put("n2", 4);
    
        final Struct valueB1 = new Struct(schemaInnerB1)
        .put("name", "he")
        .put("age", 23)
        .put("other1", valueB2)
        .put("other2", valueB3);
    
        final Struct valueB = new Struct(schemaB)
        .put("updated", "123")
        .put("after", valueB1);
    
        final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
        thrown.expect(SchemaBuilderException.class);
        thrown.expectMessage("Cannot create field because of field name duplication age");
        SinkRecord recordBExpended = buffer.expand(recordB);
      }

  @Test
  public void testGPUpsertThenDelete() throws SQLException {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", "jdbc:postgresql://localhost/postgres?user=postgres&password=postgres");
    props.put("auto.create", false);
    //props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    props.put("insert.mode", "upsert"); 
    props.put("delete.enabled", true); 
    props.put("pk.mode", "record_key"); 
    props.put("pk.fields", "id"); 
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    //final String url ="jdbc:postgresql://localhost/postgres?user=postgres&password=postgres";
    final DatabaseDialect dbDialect = new GreenplumDatabaseDialect(config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "t_avro");

    final CachedConnectionProvider cachedConnectionProvider = new CachedConnectionProvider(dbDialect);
    final Connection connection = cachedConnectionProvider.getConnection();
    if (connection.getAutoCommit()){
        connection.setAutoCommit(false);
    }

    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);

    final Schema keySchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .build();

    final Struct key = new Struct(keySchema)
        .put("id", 1);

    final Schema valueSchema = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("ts", Schema.INT32_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    final Struct value = new Struct(valueSchema)
        .put("id", 1)
        .put("ts", 1562868)
        .put("age", 23)
        .put("name", "Frank");

    final SinkRecord record = new SinkRecord(tableId.tableName(), 0, keySchema, key, valueSchema, value, 0);
    final SinkRecord recordDelete = new SinkRecord(tableId.tableName(), 0, keySchema, key, null, null, 2);

    final Schema keySchemaB = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .build();

    final Struct keyB = new Struct(keySchemaB)
        .put("id", 2);

    final Schema valueSchemaB = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("ts", Schema.INT32_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .build();

    final Struct valueB = new Struct(valueSchemaB)
        .put("id", 2)
        .put("ts", 100)
        .put("age", 20)
        .put("name", "nick");
    final SinkRecord recordB = new SinkRecord(tableId.tableName(), 0, keySchemaB, keyB, valueSchemaB, valueB, 1);

    try {
        assertEquals(Collections.emptyList(), buffer.add(record));
        assertEquals(Collections.emptyList(), buffer.add(recordB));
        assertEquals(Collections.emptyList(), buffer.add(recordDelete));
        assertEquals(Arrays.asList(record, recordB, recordDelete), buffer.flush());
        //assertEquals(Arrays.asList(record, recordB), buffer.flush());
    } catch (SQLException sqle) {
        System.out.println(sqle.getMessage());
    }
        connection.commit();
        cachedConnectionProvider.close();
  }

  @Test
  public void correctBatching() throws SQLException {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema schemaA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    final Struct valueA = new Struct(schemaA)
        .put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("name", "cuba")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, null, null, schemaB, valueB, 1);

    // test records are batched correctly based on schema equality as records are added
    //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    assertEquals(Arrays.asList(recordA, recordA, recordA), buffer.add(recordB));

    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }

  @Test(expected = ConfigException.class)
  public void configParsingFailsIfDeleteWithWrongPKMode() {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("delete.enabled", true);
    props.put("insert.mode", "upsert");
    props.put("pk.mode", "kafka"); // wrong pk mode for deletes
    new JdbcSinkConfig(props);
  }

  @Test
  public void insertThenDeleteInBatchNoFlush() throws SQLException {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("delete.enabled", true);
    props.put("insert.mode", "upsert");
    props.put("pk.mode", "record_key");
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema keySchemaA = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .build();
    final Schema valueSchemaA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    final Struct keyA = new Struct(keySchemaA)
        .put("id", 1234L);
    final Struct valueA = new Struct(valueSchemaA)
        .put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("name", "cuba")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
    
    // test records are batched correctly based on schema equality as records are added
    //   (schemaA,schemaA,schemaA,schemaB,schemaA) -> ([schemaA,schemaA,schemaA],[schemaB],[schemaA])

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    // delete should not cause a flush (i.e. not treated as a schema change)
    assertEquals(Collections.emptyList(), buffer.add(recordADelete));

    // schema change should trigger flush
    assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordB));

    // second schema change should trigger flush
    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }

  @Test
  public void insertThenDeleteThenInsertInBatchFlush() throws SQLException {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("delete.enabled", true);
    props.put("insert.mode", "upsert");
    props.put("pk.mode", "record_key");
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema keySchemaA = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .build();
    final Schema valueSchemaA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    final Struct keyA = new Struct(keySchemaA)
        .put("id", 1234L);
    final Struct valueA = new Struct(valueSchemaA)
        .put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("name", "cuba")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);

    assertEquals(Collections.emptyList(), buffer.add(recordA));
    assertEquals(Collections.emptyList(), buffer.add(recordA));

    // delete should not cause a flush (i.e. not treated as a schema change)
    assertEquals(Collections.emptyList(), buffer.add(recordADelete));

    // insert after delete should flush to insure insert isn't lost in batching
    assertEquals(Arrays.asList(recordA, recordA, recordADelete), buffer.add(recordA));

    // schema change should trigger flush
    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

    // second schema change should trigger flush
    assertEquals(Collections.singletonList(recordB), buffer.add(recordA));

    assertEquals(Collections.singletonList(recordA), buffer.flush());
  }

  @Test
  public void testMultipleDeletesBatchedTogether() throws SQLException {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("delete.enabled", true);
    props.put("insert.mode", "upsert");
    props.put("pk.mode", "record_key");
    props.put("batch.size", 1000); // sufficiently high to not cause flushes due to buffer being full
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructure = new DbStructure(dbDialect);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, sqliteHelper.connection);

    final Schema keySchemaA = SchemaBuilder.struct()
        .field("id", Schema.INT64_SCHEMA)
        .build();
    final Schema valueSchemaA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .build();
    final Struct keyA = new Struct(keySchemaA)
        .put("id", 1234L);
    final Struct valueA = new Struct(valueSchemaA)
        .put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, keySchemaA, keyA, valueSchemaA, valueA, 0);
    final SinkRecord recordADelete = new SinkRecord("dummy", 0, keySchemaA, keyA, null, null, 0);

    final Schema schemaB = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("name", "cuba")
        .put("age", 4);
    final SinkRecord recordB = new SinkRecord("dummy", 1, keySchemaA, keyA, schemaB, valueB, 1);
    final SinkRecord recordBDelete = new SinkRecord("dummy", 1, keySchemaA, keyA, null, null, 1);

    assertEquals(Collections.emptyList(), buffer.add(recordA));

    // schema change should trigger flush
    assertEquals(Collections.singletonList(recordA), buffer.add(recordB));

    // deletes should not cause a flush (i.e. not treated as a schema change)
    assertEquals(Collections.emptyList(), buffer.add(recordADelete));
    assertEquals(Collections.emptyList(), buffer.add(recordBDelete));

    // insert after delete should flush to insure insert isn't lost in batching
    assertEquals(Arrays.asList(recordB, recordADelete, recordBDelete), buffer.add(recordB));

    assertEquals(Collections.singletonList(recordB), buffer.flush());
  }

  @Test
  public void testFlushSuccessNoInfo() throws SQLException {
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", "");
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000);
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);

    int[] batchResponse = new int[2];
    batchResponse[0] = Statement.SUCCESS_NO_INFO;
    batchResponse[1] = Statement.SUCCESS_NO_INFO;

    final DbStructure dbStructureMock = mock(DbStructure.class);
    when(dbStructureMock.createOrAmendIfNecessary(Matchers.any(JdbcSinkConfig.class),
                                                  Matchers.any(Connection.class),
                                                  Matchers.any(TableId.class),
                                                  Matchers.any(FieldsMetadata.class)))
        .thenReturn(true);

    PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
    when(preparedStatementMock.executeBatch()).thenReturn(batchResponse);

    Connection connectionMock = mock(Connection.class);
    when(connectionMock.prepareStatement(Matchers.anyString())).thenReturn(preparedStatementMock);

    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect,
                                                       dbStructureMock, connectionMock);

    final Schema schemaA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    final Struct valueA = new Struct(schemaA).put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    buffer.add(recordA);

    final Schema schemaB = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    final Struct valueB = new Struct(schemaA).put("name", "cubb");
    final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
    buffer.add(recordB);
    buffer.flush();

  }


  @Test
  public void testInsertModeUpdate() throws SQLException{
    final HashMap<Object, Object> props = new HashMap<>();
    props.put("connection.url", "");
    props.put("auto.create", true);
    props.put("auto.evolve", true);
    props.put("batch.size", 1000);
    props.put("insert.mode", "update");
    final JdbcSinkConfig config = new JdbcSinkConfig(props);

    final String url = sqliteHelper.sqliteUri();
    final DatabaseDialect dbDialect = DatabaseDialects.findBestFor(url, config);
    final DbStructure dbStructureMock = mock(DbStructure.class);
    when(dbStructureMock.createOrAmendIfNecessary(Matchers.any(JdbcSinkConfig.class),
                                                  Matchers.any(Connection.class),
                                                  Matchers.any(TableId.class),
                                                  Matchers.any(FieldsMetadata.class)))
        .thenReturn(true);

    final Connection connectionMock = mock(Connection.class);
    final TableId tableId = new TableId(null, null, "dummy");
    final BufferedRecords buffer = new BufferedRecords(config, tableId, dbDialect, dbStructureMock,
            connectionMock);

    final Schema schemaA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    final Struct valueA = new Struct(schemaA).put("name", "cuba");
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    buffer.add(recordA);

    Mockito.verify(connectionMock, Mockito.times(1)).prepareStatement(Matchers.eq("UPDATE `dummy` SET `name` = ?"));

  }
}
