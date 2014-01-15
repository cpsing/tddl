package com.taobao.tddl.optimizer.parse;

import java.util.Map;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.model.SqlType;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.DeleteNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.PutNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;

/**
 * 语法树构建结果
 */
public interface SqlAnalysisResult {

    public SqlType getSqlType();

    public String getSql();

    public ASTNode getAstNode(Map<Integer, ParameterContext> parameterSettings);

    public QueryTreeNode getQueryTreeNode(Map<Integer, ParameterContext> parameterSettings);

    public UpdateNode getUpdateNode(Map<Integer, ParameterContext> parameterSettings);

    public InsertNode getInsertNode(Map<Integer, ParameterContext> parameterSettings);

    public PutNode getReplaceNode(Map<Integer, ParameterContext> parameterSettings);

    public DeleteNode getDeleteNode(Map<Integer, ParameterContext> parameterSettings);
}
