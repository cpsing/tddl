package com.taobao.tddl.repo.mysql.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.handler.QueryHandler;
import com.taobao.tddl.executor.spi.ICommandHandler;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.cursor.SchematicMyCursor;
import com.taobao.tddl.repo.mysql.spi.DatasourceMySQLImplement;
import com.taobao.tddl.repo.mysql.spi.My_Cursor;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.utils.MysqlRepoUtils;

/**
 * mysql 查询逻辑
 * 
 * @author mengshi.sunmengshi 2013-12-6 上午11:26:49
 * @since 5.0.0
 */
public class QueryMyHandler extends QueryHandler implements ICommandHandler {

    protected IDataSourceGetter dsGetter;

    public QueryMyHandler(){
        super();
        dsGetter = new DatasourceMySQLImplement();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        if (!canComposeOneSql(executor)) {
            return super.handle(executor, executionContext);
        }

        IndexMeta indexMeta = null;
        My_JdbcHandler jdbcHandler = MysqlRepoUtils.getJdbcHandler(dsGetter, executor, executionContext);
        ICursorMeta meta = ExecUtils.convertToICursorMeta((IQueryTree) executor);
        My_Cursor my_cursor = new My_Cursor(jdbcHandler, meta, executor, executor.isStreaming());

        // if (executor.getSql() != null) {
        // // TODO shenxun : 排序信息似乎丢了啊。。
        // return my_cursor.getResultSet();
        // }

        List<IOrderBy> orderBy = null;
        if (executor instanceof IJoin) {
            orderBy = ((IJoin) executor).getOrderBys();
        } else {
            orderBy = MysqlRepoUtils.buildOrderBy(executor, indexMeta);
        }

        orderBy = ExecUtils.copyOrderBys(orderBy);

        for (IOrderBy order : orderBy) {
            if (((IQueryTree) executor).getAlias() != null) {
                order.getColumn().setTableName(((IQueryTree) executor).getAlias());
            }
            if (order.getColumn().getAlias() != null) order.getColumn().setColumnName(order.getColumn().getAlias());
        }

        if (GeneralUtil.getExtraCmdBoolean(executionContext.getExtraCmds(), ExtraCmd.EXECUTE_QUERY_WHEN_CREATED, false)) {
            my_cursor.init();
        }
        return new SchematicMyCursor(my_cursor, meta, orderBy);
    }

    // private IQuery findLeafQuery(IDataNodeExecutor executor) {
    // IQuery iq = (IQuery) executor;
    // if (iq.getSubQuery() == null) {
    // return iq;
    // } else {
    // return findLeafQuery(iq.getSubQuery());
    // }
    // }

    protected boolean canComposeOneSql(IDataNodeExecutor executor) {
        if (executor instanceof IQuery) {
            IQuery iq = (IQuery) executor;
            IQueryTree iqc = iq.getSubQuery();
            if (iqc == null) {
                return true;
            }
            String groupNode1 = iq.getDataNode();
            String groupNode2 = iqc.getDataNode();
            if (TStringUtil.equals(groupNode1, groupNode2)) {
                return isConsistent(iqc, groupNode1);
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private boolean isConsistent(IQueryTree iqc, String groupNode1) {
        if (iqc instanceof IQuery) {
            IQuery iq = (IQuery) iqc;
            IQueryTree iqc1 = iq.getSubQuery();
            if (iqc1 == null) {
                return true;
            } else {
                String groupNode2 = iqc.getDataNode();
                if (TStringUtil.equals(groupNode1, groupNode2)) {
                    return isConsistent(iqc, groupNode1);
                } else {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

}
