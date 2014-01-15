package com.taobao.tddl.optimizer.parse.cobar;

import java.util.List;
import java.util.Map;

import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.SysVarPrimary;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate.Type;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowIndex;
import com.alibaba.cobar.parser.ast.stmt.ddl.DDLStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLSelectStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.cobar.parser.util.Pair;
import com.alibaba.cobar.parser.visitor.SQLASTVisitor;
import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.parse.SqlAnalysisResult;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlDeleteVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlInsertVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlReplaceIntoVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlSelectVisitor;
import com.taobao.tddl.optimizer.parse.cobar.visitor.MySqlUpdateVisitor;

/**
 * 基于cobar构造的parse结果
 */
public class CobarSqlAnalysisResult implements SqlAnalysisResult {

    private SqlType       sqlType;
    private SQLASTVisitor visitor;
    private QueryTreeNode dalQueryTreeNode;
    private SQLStatement  statement;
    private boolean       hasVisited;
    private String        sql;

    public void build(String sql, SQLStatement statement) {
        if (sql != null) {
            this.sql = sql;
            this.statement = statement;
            if (statement instanceof DMLSelectStatement) {
                if (isSysSelectStatement((DMLSelectStatement) statement, sql)) {
                    sqlType = SqlType.SHOW_WITHOUT_TABLE;
                    return;
                }

                sqlType = SqlType.SELECT;
                visitor = new MySqlSelectVisitor();
            } else if (statement instanceof DMLUpdateStatement) {
                sqlType = SqlType.UPDATE;
                visitor = new MySqlUpdateVisitor();
            } else if (statement instanceof DMLDeleteStatement) {
                sqlType = SqlType.DELETE;
                visitor = new MySqlDeleteVisitor();
            } else if (statement instanceof DMLInsertStatement) {
                sqlType = SqlType.INSERT;
                visitor = new MySqlInsertVisitor();
            } else if (statement instanceof DMLReplaceStatement) {
                sqlType = SqlType.REPLACE;
                visitor = new MySqlReplaceIntoVisitor();
            } else if (statement instanceof DDLStatement) {
                throw new IllegalArgumentException("tddl not support DDL statement:'" + sql + "'");
            } else if (statement instanceof ShowCreate) {
                if (((ShowCreate) statement).getType() == Type.TABLE) {
                    dalQueryTreeNode = new TableNode(((ShowCreate) statement).getId().getIdTextUpUnescape());
                    sqlType = SqlType.SHOW_WITH_TABLE;
                } else {
                    sqlType = SqlType.SHOW_WITHOUT_TABLE;
                }
            } else if (statement instanceof ShowColumns) {
                dalQueryTreeNode = new TableNode(((ShowColumns) statement).getTable().getIdTextUpUnescape());
                sqlType = SqlType.SHOW_WITH_TABLE;
            } else if (statement instanceof ShowIndex) {
                dalQueryTreeNode = new TableNode(((ShowIndex) statement).getTable().getIdTextUpUnescape());
                sqlType = SqlType.SHOW_WITH_TABLE;
            } else {
                sqlType = SqlType.SHOW_WITHOUT_TABLE;
            }
        }
    }

    public void visited() {
        if (hasVisited == false && statement != null && visitor != null) {
            statement.accept(visitor);
            hasVisited = true;
        }
    }

    private boolean isSysSelectStatement(DMLSelectStatement statement, String sql) {
        TableReferences tables = statement.getTables();
        if (tables == null) {
            List<Pair<Expression, String>> exprs = statement.getSelectExprList();
            if (exprs != null && exprs.size() == 1) {
                if (exprs.get(0).getKey() instanceof SysVarPrimary) {
                    SysVarPrimary pri = (SysVarPrimary) exprs.get(0).getKey();
                    if ("AUTOCOMMIT".equals(pri.getVarTextUp()) || "LASTINSERTID".equals(pri.getVarTextUp())) {
                        throw new IllegalArgumentException("not support such SysVarPrimary:'" + pri.getVarTextUp()
                                                           + "'");
                    } else {
                        return true;
                    }
                } else {
                    return true;
                }
            } else if (exprs != null && exprs.size() > 1) {
                throw new IllegalArgumentException("not support multi SysVarPrimary:'" + sql + "'");
            } else {
                throw new IllegalArgumentException("not supported sql:'" + sql + "'");
            }
        } else {
            return false;
        }
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public QueryTreeNode getQueryTreeNode(Map<Integer, ParameterContext> parameterSettings) {
        visited();
        if (dalQueryTreeNode == null) {
            return ((MySqlSelectVisitor) visitor).getTableNode();
        } else {
            return this.dalQueryTreeNode;
        }
    }

    public UpdateNode getUpdateNode(Map<Integer, ParameterContext> parameterSettings) {
        visited();
        return (UpdateNode) ((MySqlUpdateVisitor) visitor).getUpdateNode().setParameterSettings(parameterSettings);
    }

    public InsertNode getInsertNode(Map<Integer, ParameterContext> parameterSettings) {
        visited();
        return (InsertNode) ((MySqlInsertVisitor) visitor).getInsertNode().setParameterSettings(parameterSettings);
    }

    public PutNode getReplaceNode(Map<Integer, ParameterContext> parameterSettings) {
        visited();
        return (PutNode) ((MySqlReplaceIntoVisitor) visitor).getReplaceNode().setParameterSettings(parameterSettings);
    }

    public DeleteNode getDeleteNode(Map<Integer, ParameterContext> parameterSettings) {
        visited();
        return (DeleteNode) ((MySqlDeleteVisitor) visitor).getDeleteNode().setParameterSettings(parameterSettings);
    }

    public ASTNode getAstNode(Map<Integer, ParameterContext> parameterSettings) {
        if (sqlType == SqlType.SELECT || sqlType == SqlType.SHOW_WITH_TABLE) {
            return getQueryTreeNode(parameterSettings);
        } else if (sqlType == SqlType.UPDATE) {
            return getUpdateNode(parameterSettings);
        } else if (sqlType == SqlType.INSERT) {
            return getInsertNode(parameterSettings);
        } else if (sqlType == SqlType.REPLACE) {
            return getReplaceNode(parameterSettings);
        } else if (sqlType == SqlType.DELETE) {
            return getDeleteNode(parameterSettings);
        }

        throw new NotSupportException(sqlType.toString());
    }

    public SQLStatement getStatement() {
        return statement;
    }

    public String getSql() {
        return this.sql;
    }

}
