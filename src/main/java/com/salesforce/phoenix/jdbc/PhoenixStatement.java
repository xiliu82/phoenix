/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.jdbc;

import java.io.IOException;
import java.io.Reader;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.BindManager;
import com.salesforce.phoenix.compile.ColumnProjector;
import com.salesforce.phoenix.compile.CreateIndexCompiler;
import com.salesforce.phoenix.compile.CreateTableCompiler;
import com.salesforce.phoenix.compile.DeleteCompiler;
import com.salesforce.phoenix.compile.ExplainPlan;
import com.salesforce.phoenix.compile.ExpressionProjector;
import com.salesforce.phoenix.compile.MutationPlan;
import com.salesforce.phoenix.compile.QueryCompiler;
import com.salesforce.phoenix.compile.QueryPlan;
import com.salesforce.phoenix.compile.RowProjector;
import com.salesforce.phoenix.compile.StatementPlan;
import com.salesforce.phoenix.compile.UpsertCompiler;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.execute.MutationState;
import com.salesforce.phoenix.expression.RowKeyColumnExpression;
import com.salesforce.phoenix.iterate.MaterializedResultIterator;
import com.salesforce.phoenix.parse.AddColumnStatement;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.AlterIndexStatement;
import com.salesforce.phoenix.parse.BindableStatement;
import com.salesforce.phoenix.parse.ColumnDef;
import com.salesforce.phoenix.parse.ColumnName;
import com.salesforce.phoenix.parse.CreateIndexStatement;
import com.salesforce.phoenix.parse.CreateTableStatement;
import com.salesforce.phoenix.parse.DeleteStatement;
import com.salesforce.phoenix.parse.DropColumnStatement;
import com.salesforce.phoenix.parse.DropIndexStatement;
import com.salesforce.phoenix.parse.DropTableStatement;
import com.salesforce.phoenix.parse.ExplainStatement;
import com.salesforce.phoenix.parse.HintNode;
import com.salesforce.phoenix.parse.LimitNode;
import com.salesforce.phoenix.parse.NamedNode;
import com.salesforce.phoenix.parse.NamedTableNode;
import com.salesforce.phoenix.parse.OrderByNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.PrimaryKeyConstraint;
import com.salesforce.phoenix.parse.SQLParser;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.ShowTablesStatement;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.parse.TableNode;
import com.salesforce.phoenix.parse.UpsertStatement;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;
import com.salesforce.phoenix.query.Scanner;
import com.salesforce.phoenix.query.WrappedScanner;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.ExecuteQueryNotApplicableException;
import com.salesforce.phoenix.schema.ExecuteUpdateNotApplicableException;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDatum;
import com.salesforce.phoenix.schema.PIndexState;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.RowKeyValueAccessor;
import com.salesforce.phoenix.schema.tuple.SingleKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.SQLCloseable;
import com.salesforce.phoenix.util.SQLCloseables;
import com.salesforce.phoenix.util.SchemaUtil;
import com.salesforce.phoenix.util.ServerUtil;


/**
 * 
 * JDBC Statement implementation of Phoenix.
 * Currently only the following methods are supported:
 * - {@link #executeQuery(String)}
 * - {@link #executeUpdate(String)}
 * - {@link #execute(String)}
 * - {@link #getResultSet()}
 * - {@link #getUpdateCount()}
 * - {@link #close()}
 * The Statement only supports the following options:
 * - ResultSet.FETCH_FORWARD
 * - ResultSet.TYPE_FORWARD_ONLY
 * - ResultSet.CLOSE_CURSORS_AT_COMMIT
 * 
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixStatement implements Statement, SQLCloseable, com.salesforce.phoenix.jdbc.Jdbc7Shim.Statement {
    public enum UpdateOperation {
        DELETED("deleted"),
        UPSERTED("upserted");
        
        private final String toString;
        UpdateOperation(String toString) {
            this.toString = toString;
        }
        
        @Override
        public String toString() {
            return toString;
        }
        };

    protected final PhoenixConnection connection;
    private static final int NO_UPDATE = -1;
    private List<PhoenixResultSet> resultSets = new ArrayList<PhoenixResultSet>();
    private QueryPlan lastQueryPlan;
    private PhoenixResultSet lastResultSet;
    private int lastUpdateCount = NO_UPDATE;
    private UpdateOperation lastUpdateOperation;
    private boolean isClosed = false;
    private ResultSetMetaData resultSetMetaData;
    private int maxRows;
    
    
    public PhoenixStatement(PhoenixConnection connection) {
        this.connection = connection;
    }
    
    protected List<PhoenixResultSet> getResultSets() {
        return resultSets;
    }
    
    protected PhoenixResultSet newResultSet(Scanner scanner) throws SQLException {
        return new PhoenixResultSet(scanner, PhoenixStatement.this);
    }
    
    protected static interface ExecutableStatement extends BindableStatement {
        public boolean execute() throws SQLException;
        public int executeUpdate() throws SQLException;
        public PhoenixResultSet executeQuery() throws SQLException;
        public ResultSetMetaData getResultSetMetaData() throws SQLException;
        public StatementPlan optimizePlan() throws SQLException;
        public StatementPlan compilePlan() throws SQLException;
    }
    
    protected static interface MutatableStatement extends ExecutableStatement {
        @Override
        public MutationPlan optimizePlan() throws SQLException;
    }
    
    private class ExecutableSelectStatement extends SelectStatement implements ExecutableStatement {
        private ExecutableSelectStatement(List<? extends TableNode> from, HintNode hint, boolean isDistinct, List<AliasedNode> select, ParseNode where,
                List<ParseNode> groupBy, ParseNode having, List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {
            super(from, hint, isDistinct, select, where, groupBy, having, orderBy, limit, bindCount, isAggregate);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            QueryPlan plan = optimizePlan();
            Scanner scanner = plan.getScanner();
            PhoenixResultSet rs = newResultSet(scanner);
            resultSets.add(rs);
            lastResultSet = rs;
            lastUpdateCount = NO_UPDATE;
            lastUpdateOperation = null;
            return rs;
        }

        @Override
        public boolean execute() throws SQLException {
            executeQuery();
            return true;
        }

        @Override
        public int executeUpdate() throws SQLException {
            throw new ExecuteUpdateNotApplicableException(this.toString());
        }

        @Override
        public QueryPlan optimizePlan() throws SQLException {
            return lastQueryPlan = connection.getQueryServices().getOptimizer().optimize(this, PhoenixStatement.this);
        }
        
        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new QueryCompiler(PhoenixStatement.this).compile(this);
        }
        
        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            if (resultSetMetaData == null) {
                // Just compile top level query without optimizing to get ResultSetMetaData
                QueryPlan plan = new QueryCompiler(PhoenixStatement.this).compile(this);
                resultSetMetaData = new PhoenixResultSetMetaData(connection, plan.getProjector());
            }
            return resultSetMetaData;
        }
    }
    
    private int executeMutation(MutationPlan plan) throws SQLException {
        // Note that the upsert select statements will need to commit any open transaction here,
        // since they'd update data directly from coprocessors, and should thus operate on
        // the latest state
        MutationState state = plan.execute();
        connection.getMutationState().join(state);
        if (connection.getAutoCommit()) {
            connection.commit();
        }
        lastResultSet = null;
        lastQueryPlan = null;
        // Unfortunately, JDBC uses an int for update count, so we
        // just max out at Integer.MAX_VALUE
        long updateCount = state.getUpdateCount();
        lastUpdateCount = (int)Math.min(Integer.MAX_VALUE, updateCount);
        return lastUpdateCount;
    }
    
    private class ExecutableUpsertStatement extends UpsertStatement implements MutatableStatement {
        private ExecutableUpsertStatement(NamedTableNode table, HintNode hintNode, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount) {
            super(table, hintNode, columns, values, select, bindCount);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("upsert", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            lastUpdateOperation = UpdateOperation.UPSERTED;
            return executeMutation(optimizePlan());
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public MutationPlan compilePlan() throws SQLException {
            UpsertCompiler compiler = new UpsertCompiler(PhoenixStatement.this);
            return compiler.compile(this);
        }
        
        @Override
        public MutationPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }
    
    private class ExecutableDeleteStatement extends DeleteStatement implements MutatableStatement {
        private ExecutableDeleteStatement(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
            super(table, hint, whereNode, orderBy, limit, bindCount);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("delete", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            lastUpdateOperation = UpdateOperation.DELETED;
            return executeMutation(optimizePlan());
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public MutationPlan compilePlan() throws SQLException {
            DeleteCompiler compiler = new DeleteCompiler(PhoenixStatement.this);
            return compiler.compile(this);
        }
        
        @Override
        public MutationPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }
    
    private class ExecutableCreateTableStatement extends CreateTableStatement implements ExecutableStatement {
        ExecutableCreateTableStatement(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columnDefs, PrimaryKeyConstraint pkConstraint, List<ParseNode> splitNodes, PTableType tableType, boolean ifNotExists, int bindCount) {
            super(tableName, props, columnDefs, pkConstraint, splitNodes, tableType, ifNotExists, bindCount);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("CREATE TABLE", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MutationPlan plan = optimizePlan();
            MutationState state = plan.execute();
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.UPSERTED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public MutationPlan compilePlan() throws SQLException {
            CreateTableCompiler compiler = new CreateTableCompiler(PhoenixStatement.this);
            return compiler.compile(this);
        }
        
        @Override
        public MutationPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableCreateIndexStatement extends CreateIndexStatement implements ExecutableStatement {

        public ExecutableCreateIndexStatement(NamedNode indexName, NamedTableNode dataTable, PrimaryKeyConstraint pkConstraint, List<ColumnName> includeColumns, List<ParseNode> splits,
                ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, int bindCount) {
            super(indexName, dataTable, pkConstraint, includeColumns, splits, props, ifNotExists, bindCount);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("CREATE INDEX", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MutationPlan plan = optimizePlan();
            MutationState state = plan.execute();
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.UPSERTED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public MutationPlan compilePlan() throws SQLException {
            CreateIndexCompiler compiler = new CreateIndexCompiler(PhoenixStatement.this);
            return compiler.compile(this);
        }
        
        @Override
        public MutationPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableDropTableStatement extends DropTableStatement implements ExecutableStatement {

        ExecutableDropTableStatement(TableName tableName, PTableType tableType, boolean ifExists) {
            super(tableName, tableType, ifExists);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("DROP TABLE", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MetaDataClient client = new MetaDataClient(connection);
            MutationState state = client.dropTable(this);
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.DELETED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new StatementPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DROP TABLE"));
                }
            };
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableDropIndexStatement extends DropIndexStatement implements ExecutableStatement {

        public ExecutableDropIndexStatement(NamedNode indexName, TableName tableName, boolean ifExists) {
            super(indexName, tableName, ifExists);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("DROP INDEX", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MetaDataClient client = new MetaDataClient(connection);
            MutationState state = client.dropIndex(this);
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.DELETED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new StatementPlan() {
                
                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }
                
                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("DROP INDEX"));
                }
            };
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableAlterIndexStatement extends AlterIndexStatement implements ExecutableStatement {

        public ExecutableAlterIndexStatement(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state) {
            super(indexTableNode, dataTableName, ifExists, state);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("ALTER INDEX", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MetaDataClient client = new MetaDataClient(connection);
            MutationState state = client.alterIndex(this);
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.UPSERTED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new StatementPlan() {
                
                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }
                
                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER INDEX"));
                }
            };
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableAddColumnStatement extends AddColumnStatement implements ExecutableStatement {

        ExecutableAddColumnStatement(NamedTableNode table, List<ColumnDef> columnDefs, boolean ifNotExists, Map<String, Object> props) {
            super(table, columnDefs, ifNotExists, props);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("ALTER TABLE", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MetaDataClient client = new MetaDataClient(connection);
            MutationState state = client.addColumn(this);
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.UPSERTED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new StatementPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER TABLE ADD COLUMN"));
                }
            };
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableDropColumnStatement extends DropColumnStatement implements ExecutableStatement {

        ExecutableDropColumnStatement(NamedTableNode table, ColumnName columnRef, boolean ifExists) {
            super(table, columnRef, ifExists);
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("ALTER TABLE", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            MetaDataClient client = new MetaDataClient(connection);
            MutationState state = client.dropColumn(this);
            lastQueryPlan = null;
            lastResultSet = null;
            lastUpdateCount = (int)Math.min(state.getUpdateCount(), Integer.MAX_VALUE);
            lastUpdateOperation = UpdateOperation.UPSERTED;
            return lastUpdateCount;
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new StatementPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return new PhoenixParameterMetaData(0);
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("ALTER TABLE DROP COLUMN"));
                }
            };
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private static final byte[] EXPLAIN_PLAN_FAMILY = QueryConstants.SINGLE_COLUMN_FAMILY;
    private static final byte[] EXPLAIN_PLAN_COLUMN = PDataType.VARCHAR.toBytes("Plan");
    private static final String EXPLAIN_PLAN_ALIAS = "PLAN";
    private static final String EXPLAIN_PLAN_TABLE_NAME = "PLAN_TABLE";
    private static final PDatum EXPLAIN_PLAN_DATUM = new PDatum() {
        @Override
        public boolean isNullable() {
            return false;
        }
        @Override
        public PDataType getDataType() {
            return PDataType.VARCHAR;
        }
        @Override
        public Integer getByteSize() {
            return null;
        }
        @Override
        public Integer getMaxLength() {
            return null;
        }
        @Override
        public Integer getScale() {
            return null;
        }
		@Override
		public ColumnModifier getColumnModifier() {
			return null;
		}
    };
    private static final RowProjector EXPLAIN_PLAN_ROW_PROJECTOR = new RowProjector(Arrays.<ColumnProjector>asList(
            new ExpressionProjector(EXPLAIN_PLAN_ALIAS, EXPLAIN_PLAN_TABLE_NAME, 
                    new RowKeyColumnExpression(EXPLAIN_PLAN_DATUM,
                            new RowKeyValueAccessor(Collections.<PDatum>singletonList(EXPLAIN_PLAN_DATUM), 0)), false)
            ), 0, true);
    private class ExecutableExplainStatement extends ExplainStatement implements ExecutableStatement {

        public ExecutableExplainStatement(BindableStatement statement) {
            super(statement);
        }

        @Override
        public ExecutableStatement getStatement() {
            return (ExecutableStatement) super.getStatement();
        }
        
        @Override
        public int getBindCount() {
            return getStatement().getBindCount();
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            StatementPlan plan = getStatement().optimizePlan();
            List<String> planSteps = plan.getExplainPlan().getPlanSteps();
            List<Tuple> tuples = Lists.newArrayListWithExpectedSize(planSteps.size());
            for (String planStep : planSteps) {
                Tuple tuple = new SingleKeyValueTuple(KeyValueUtil.newKeyValue(PDataType.VARCHAR.toBytes(planStep), EXPLAIN_PLAN_FAMILY, EXPLAIN_PLAN_COLUMN, MetaDataProtocol.MIN_TABLE_TIMESTAMP, ByteUtil.EMPTY_BYTE_ARRAY));
                tuples.add(tuple);
            }
            Scanner scanner = new WrappedScanner(new MaterializedResultIterator(tuples),EXPLAIN_PLAN_ROW_PROJECTOR);
            PhoenixResultSet rs = new PhoenixResultSet(scanner, new PhoenixStatement(connection));
            lastResultSet = rs;
            lastQueryPlan = null;
            lastUpdateCount = NO_UPDATE;
            return rs;
        }

        @Override
        public boolean execute() throws SQLException {
            executeQuery();
            return true;
        }

        @Override
        public int executeUpdate() throws SQLException {
            throw new ExecuteUpdateNotApplicableException("ALTER TABLE", this.toString());
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return new PhoenixResultSetMetaData(connection, EXPLAIN_PLAN_ROW_PROJECTOR);
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return StatementPlan.EMPTY_PLAN;
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    private class ExecutableShowTablesStatement extends ShowTablesStatement implements ExecutableStatement {

        ExecutableShowTablesStatement() {
        }

        @Override
        public PhoenixResultSet executeQuery() throws SQLException {
            throw new ExecuteQueryNotApplicableException("SHOW TABLES", this.toString());
        }

        @Override
        public boolean execute() throws SQLException {
            executeUpdate();
            return false;
        }

        @Override
        public int executeUpdate() throws SQLException {
            ResultSet rs = null;
            try {
                rs = connection.getMetaData().getTables(null,null,null,null);
                while (rs.next()) {
                    String schema = rs.getString(2);
                    String table = rs.getString(3);
                    SchemaUtil.getTableName(schema,table);
                }
                return 0;
            } finally {
                if(rs != null) {
                    rs.close();
                }
            }
            
        }

        @Override
        public ResultSetMetaData getResultSetMetaData() throws SQLException {
            return null;
        }

        @Override
        public StatementPlan compilePlan() throws SQLException {
            return new StatementPlan() {

                @Override
                public ParameterMetaData getParameterMetaData() {
                    return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
                }

                @Override
                public ExplainPlan getExplainPlan() throws SQLException {
                    return new ExplainPlan(Collections.singletonList("SHOW TABLES"));
                }
            };
        }
        
        @Override
        public StatementPlan optimizePlan() throws SQLException {
            return compilePlan();
        }
    }

    protected class ExecutableNodeFactory extends ParseNodeFactory {
        @Override
        public ExecutableSelectStatement select(List<? extends TableNode> from, HintNode hint, boolean isDistinct, List<AliasedNode> select,
                                                ParseNode where, List<ParseNode> groupBy, ParseNode having,
                                                List<OrderByNode> orderBy, LimitNode limit, int bindCount, boolean isAggregate) {
            return new ExecutableSelectStatement(from, hint, isDistinct, select, where, groupBy == null ? Collections.<ParseNode>emptyList() : groupBy, having, orderBy == null ? Collections.<OrderByNode>emptyList() : orderBy, limit, bindCount, isAggregate);
        }
        
        @Override
        public ExecutableUpsertStatement upsert(NamedTableNode table, HintNode hintNode, List<ColumnName> columns, List<ParseNode> values, SelectStatement select, int bindCount) {
            return new ExecutableUpsertStatement(table, hintNode, columns, values, select, bindCount);
        }
        
        @Override
        public ExecutableDeleteStatement delete(NamedTableNode table, HintNode hint, ParseNode whereNode, List<OrderByNode> orderBy, LimitNode limit, int bindCount) {
            return new ExecutableDeleteStatement(table, hint, whereNode, orderBy, limit, bindCount);
        }
        
        @Override
        public CreateTableStatement createTable(TableName tableName, ListMultimap<String,Pair<String,Object>> props, List<ColumnDef> columns, PrimaryKeyConstraint pkConstraint, List<ParseNode> splits, PTableType tableType, boolean ifNotExists, int bindCount) {
            return new ExecutableCreateTableStatement(tableName, props, columns, pkConstraint, splits, tableType, ifNotExists, bindCount);
        }
        
        @Override
        public CreateIndexStatement createIndex(NamedNode indexName, NamedTableNode dataTable, PrimaryKeyConstraint pkConstraint, List<ColumnName> includeColumns, List<ParseNode> splits, ListMultimap<String,Pair<String,Object>> props, boolean ifNotExists, int bindCount) {
            return new ExecutableCreateIndexStatement(indexName, dataTable, pkConstraint, includeColumns, splits, props, ifNotExists, bindCount);
        }
        
        @Override
        public AddColumnStatement addColumn(NamedTableNode table,  List<ColumnDef> columnDefs, boolean ifNotExists, Map<String,Object> props) {
            return new ExecutableAddColumnStatement(table, columnDefs, ifNotExists, props);
        }
        
        @Override
        public DropColumnStatement dropColumn(NamedTableNode table,  ColumnName columnNode, boolean ifExists) {
            return new ExecutableDropColumnStatement(table, columnNode, ifExists);
        }
        
        @Override
        public DropTableStatement dropTable(TableName tableName, PTableType tableType, boolean ifExists) {
            return new ExecutableDropTableStatement(tableName, tableType, ifExists);
        }
        
        @Override
        public DropIndexStatement dropIndex(NamedNode indexName, TableName tableName, boolean ifExists) {
            return new ExecutableDropIndexStatement(indexName, tableName, ifExists);
        }
        
        @Override
        public AlterIndexStatement alterIndex(NamedTableNode indexTableNode, String dataTableName, boolean ifExists, PIndexState state) {
            return new ExecutableAlterIndexStatement(indexTableNode, dataTableName, ifExists, state);
        }
        
        @Override
        public ExplainStatement explain(BindableStatement statement) {
            return new ExecutableExplainStatement(statement);
        }

        @Override
        public ShowTablesStatement showTables() {
            return new ExecutableShowTablesStatement();
        }
    }
    
    static class PhoenixStatementParser extends SQLParser {
        PhoenixStatementParser(String query, ParseNodeFactory nodeFactory) throws IOException {
            super(query, nodeFactory);
        }

        PhoenixStatementParser(Reader reader) throws IOException {
            super(reader);
        }
        
        @Override
        public ExecutableStatement nextStatement(ParseNodeFactory nodeFactory) throws SQLException {
            return (ExecutableStatement) super.nextStatement(nodeFactory);
        }

        @Override
        public ExecutableStatement parseStatement() throws SQLException {
            return (ExecutableStatement) super.parseStatement();
        }
    }
    
    public Format getFormatter(PDataType type) {
        return connection.getFormatter(type);
    }
    
    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        // this exception has a bad interaction with c3p0, simply ignore.
        //throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void close() throws SQLException {
        try {
            List<PhoenixResultSet> resultSets = this.resultSets;
            // Create new list so that remove of the PhoenixResultSet
            // during closeAll doesn't needless do a linear search
            // on this list.
            this.resultSets = Lists.newArrayList();
            SQLCloseables.closeAll(resultSets);
        } finally {
            try {
                connection.removeStatement(this);
            } finally {
                isClosed = true;
            }
        }
    }

    public List<Object> getParameters() {
        return Collections.<Object>emptyList();
    }
    
    protected void throwIfUnboundParameters() throws SQLException {
        int i = 0;
        for (Object param : getParameters()) {
            if (param == BindManager.UNBOUND_PARAMETER) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_VALUE_UNBOUND)
                    .setMessage("Parameter " + (i + 1) + " is unbound").build().buildException();
            }
            i++;
        }
    }
    
    protected ExecutableStatement parseStatement(String sql) throws SQLException {
        PhoenixStatementParser parser = null;
        try {
            parser = new PhoenixStatementParser(sql, new ExecutableNodeFactory());
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
        ExecutableStatement statement = parser.parseStatement();
        return statement;
    }
    
    @Override
    public boolean execute(String sql) throws SQLException {
        throwIfUnboundParameters();
        return parseStatement(sql).execute();
    }

    public QueryPlan optimizeQuery(String sql) throws SQLException {
        throwIfUnboundParameters();
        return (QueryPlan)parseStatement(sql).optimizePlan();
    }

    public QueryPlan compileQuery(String sql) throws SQLException {
        throwIfUnboundParameters();
        return (QueryPlan)parseStatement(sql).compilePlan();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throwIfUnboundParameters();
        return parseStatement(sql).executeQuery();
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throwIfUnboundParameters();
        return parseStatement(sql).executeUpdate();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PhoenixConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return connection.getQueryServices().getProps().getInt(QueryServices.SCAN_CACHE_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_SCAN_CACHE_SIZE);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0; // TODO: 4000?
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return connection.getQueryServices().getProps().getInt(QueryServices.KEEP_ALIVE_MS_ATTRIB, 0) / 1000;
    }

    // For testing
    public QueryPlan getQueryPlan() {
        return lastQueryPlan;
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet rs = lastResultSet;
        lastResultSet = null;
        return rs;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // TODO: not sure this matters
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    public UpdateOperation getUpdateOperation() {
        return lastUpdateOperation;
    }
    
    @Override
    public int getUpdateCount() throws SQLException {
        int updateCount = lastUpdateCount;
        // Only first call can get the update count, otherwise
        // some SQL clients get into an infinite loop when an
        // update occurs.
        lastUpdateCount = NO_UPDATE;
        return updateCount;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // TODO: any escaping we need to do?
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // TODO: map to Scan.setBatch() ?
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        // The Phoenix setting for this is shared across all connections currently
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
