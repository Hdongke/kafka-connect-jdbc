/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for Greenplum.
 */
public class GreenplumDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link GreenplumDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(GreenplumDatabaseDialect.class.getSimpleName(), "postgresql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new GreenplumDatabaseDialect(config);
    }
  }

  private PostgreSqlDatabaseDialect pg ;

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public GreenplumDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
    pg = new PostgreSqlDatabaseDialect(config);
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Override
  public String addFieldToSchema(
      ColumnDefinition columnDefn,
      SchemaBuilder builder
  ) {
    return pg.addFieldToSchema(columnDefn, builder);
  }

  @Override
  public ColumnConverter createColumnConverter(
      ColumnMapping mapping
  ) {
    return pg.createColumnConverter(mapping);
  }

  @Override
  protected String getSqlType(SinkRecordField field) { 
    return pg.getSqlType(field);
  }

  public ExpressionBuilder upsertExpressionBuilder() {
    //TODO:
    //To be configurable
    IdentifierRules rules = new IdentifierRules(".", "'","'");
    QuoteMethod quoteSqlIdentifiers = QuoteMethod.get(
         config.getString(JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG)
     );                                              
    return rules.expressionBuilder().setQuoteIdentifiers(quoteSqlIdentifiers);
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {

    ExpressionBuilder builder = upsertExpressionBuilder();
    builder.append("SELECT fun_upsert(");
    builder.append(table);
    builder.append(",");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    return builder.toString();
  }

}