package com.taobao.tddl.optimizer.parse;

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

    public ASTNode getAstNode();

    public QueryTreeNode getQueryTreeNode();

    public UpdateNode getUpdateNode();

    public InsertNode getInsertNode();

    public PutNode getReplaceNode();

    public DeleteNode getDeleteNode();
}
