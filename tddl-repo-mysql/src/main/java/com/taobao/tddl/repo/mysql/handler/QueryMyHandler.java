package com.taobao.ustore.jdbc.common.command.handler;

import java.util.List;
import java.util.Map;

import org.codehaus.groovy.util.StringUtil;

import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ICursorMeta;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.handler.QueryHandler;
import com.taobao.tddl.executor.spi.CommandHandler;
import com.taobao.tddl.executor.spi.DataSourceGetter;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IJoin;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.CursorMyUtils;
import com.taobao.tddl.repo.mysql.cursor.SchematicMyCursor;
import com.taobao.tddl.repo.mysql.spi.My_Cursor;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;

/**
 * 这个Handler主要用于处理MySQL类似的逻辑。
 * 
 * @author Whisper
 */
public class QueryMyHandler extends QueryHandler implements CommandHandler {

    protected DataSourceGetter dsGetter;

    public QueryMyHandler(AndorContext commonConfig2){
        super(commonConfig2);
        dsGetter = new DatasourceMySQLImplement();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ISchematicCursor handle(Map<String, Comparable> context, ISchematicCursor cursor,
                                   IDataNodeExecutor executor, ExecutionContext executionContext) throws Exception {
        if (!canComposeOneSql(executor)) return super.handle(context, cursor, executor, executionContext);
        if (cursor != null) {
            throw new UnsupportedOperationException();
        }
        IndexMeta indexMeta = null;
        My_JdbcHandler jdbcHandler = CursorMyUtils.getJdbcHandler(dsGetter, context, executor, executionContext);

        ICursorMeta meta = GeneralUtil.convertToICursorMeta((IQueryCommon) executor);

        My_Cursor my_cursor = new My_Cursor(jdbcHandler,
            meta,
            executor.getGroupDataNode(),
            executor,
            executor.isStreaming(),
            this.andorContext);

        if (executor.getSql() != null) {
            // TODO shenxun : 排序信息似乎丢了啊。。
            return my_cursor.getResultSet();
        }

        List<IOrderBy> orderBy = null;
        if (executor instanceof IJoin) {
            orderBy = ((IJoin) executor).getOrderBy();
        } else
        {
            orderBy = CursorMyUtils.buildOrderBy(executor, indexMeta);
        }
        
        orderBy = GeneralUtil.copyOrderBys(orderBy);

        for (IOrderBy order : orderBy) {
            if (((IQueryCommon) executor).getAlias() != null) {
                order.getColumn().setTableName(((IQueryCommon) executor).getAlias());
            }
            if (order.getColumn().getAlias() != null) order.getColumn().setColumnName(order.getColumn().getAlias());
        }

        if ("True".equalsIgnoreCase(GeneralUtil.getExtraCmd(context,
            ExtraCmd.ExecutionExtraCmd.EXECUTE_QUERY_WHEN_CREATED))) {
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
            IQueryCommon iqc = iq.getSubQuery();
            if (iqc == null) {
                return true;
            }
            String groupNode1 = iq.getGroupDataNode();
            String groupNode2 = iqc.getGroupDataNode();
            if (StringUtil.equals(groupNode1, groupNode2)) {
                return isConsistent(iqc, groupNode1);
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private boolean isConsistent(IQueryCommon iqc, String groupNode1) {
        if (iqc instanceof IQuery) {
            IQuery iq = (IQuery) iqc;
            IQueryCommon iqc1 = iq.getSubQuery();
            if (iqc1 == null) {
                return true;
            } else {
                String groupNode2 = iqc.getGroupDataNode();
                if (StringUtil.equals(groupNode1, groupNode2)) {
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
