package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;

public class SqlConvertor {

    public SqlMergeNode convert(Map<String, String> extraCmd, IDataNodeExecutor node, boolean bindval) {
        SqlMergeNode sqlMergeNode = new SqlMergeNode();

        Map<String/* node name */, Sqls> sqlsMap = new HashMap<String, Sqls>();
        sqlMergeNode.setSubQuerys(sqlsMap);
        if (node instanceof IMerge) {
            IMerge merge = (IMerge) node;
            List<IDataNodeExecutor> subNodes = merge.getSubNode();
            if (subNodes == null) {
                throw new IllegalArgumentException("should not be here");
            }
            for (IDataNodeExecutor subNode : subNodes) {
                processOnenoMergeExecutor(bindval, sqlMergeNode, subNode);
            }
            fillOtherValues(sqlMergeNode, merge);
        } else if (node instanceof IQueryTree) {
            processOnenoMergeExecutor(bindval, sqlMergeNode, node);
            fillOtherValues(sqlMergeNode, (IQueryTree) node);
        } else if (node instanceof IPut) {
            processOnenoMergeExecutor(bindval, sqlMergeNode, node);
        }
        return sqlMergeNode;

    }

    private void fillOtherValues(SqlMergeNode sqlMergeNode, IQueryTree merge) {
        sqlMergeNode.setColumns(merge.getColumns());
        sqlMergeNode.setGroupBys(merge.getGroupBys());
        sqlMergeNode.setLimitFrom(merge.getLimitFrom());
        sqlMergeNode.setLimitTo(merge.getLimitTo());
        sqlMergeNode.setOrderBy(merge.getOrderBys());
    }

    /**
     * 因为原则上，目前简化版本的执行树只有一个merge，所以抽一个方法公用
     * 
     * @param sqlMergeNode
     * @param nodeExecutor
     */
    private void processOnenoMergeExecutor(boolean bindVal, SqlMergeNode sqlMergeNode, IDataNodeExecutor nodeExecutor) {
        if (nodeExecutor instanceof IMerge) {
            throw new IllegalStateException("sub query is merge , not supported yet .");
        }
        if (nodeExecutor instanceof IPut) {
            // 写入相关
            String nodeName = nodeExecutor.getDataNode();
            addOneSql(bindVal, sqlMergeNode, nodeName, (IPut) nodeExecutor);
        } else if (nodeExecutor instanceof IQueryTree) {
            String nodeName = nodeExecutor.getDataNode();
            // query or join
            addOneSql(bindVal, sqlMergeNode, nodeName, (IQueryTree) nodeExecutor);
        }
    }

    private void addOneSql(boolean bindVal, SqlMergeNode sqlMergeNode, String nodeName, IDataNodeExecutor oneQuery) {
        Map<String, Sqls> subQuerys = sqlMergeNode.getSubQuerys();
        Sqls sqls = subQuerys.get(nodeName);
        if (sqls == null) {
            sqls = new Sqls();
            subQuerys.put(nodeName, sqls);
        }

        if (oneQuery instanceof IQueryTree) {
            ((IQueryTree) oneQuery).setTopQuery(true);
        }
        MysqlPlanVisitorImpl visitor = new MysqlPlanVisitorImpl(oneQuery, null, null, true);
        oneQuery.accept(visitor);
        Sql sql = new Sql();
        sql.setSql(visitor.getString());
        sql.setParam(visitor.getParamMap());
        sqls.add(sql);
    }

}
