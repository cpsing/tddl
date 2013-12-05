package com.taobao.tddl.repo.mysql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import com.taobao.tddl.executor.cursor.Cursor;
import com.taobao.tddl.executor.spi.DataSourceGetter;
import com.taobao.tddl.executor.spi.ExecutionContext;
import com.taobao.tddl.executor.spi.Transaction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.spi.My_Condensable;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.spi.My_Transaction;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:08:32
 * @since 5.1.0
 */
public class CursorMyUtils {

    public static String getGroupNodeName(Cursor cursor) {
        if (cursor instanceof My_Condensable) {
            return ((My_Condensable) cursor).getGroupNodeName();
        }
        return null;
    }

    public static List<IOrderBy> buildOrderBy(IDataNodeExecutor executor, IndexMeta indexMeta) {
        IQueryTree query = ((IQueryTree) executor);
        List<IOrderBy> orderBy = null;
        orderBy = query.getOrderBys();
        if (orderBy == null || orderBy.isEmpty()) {
            List<IOrderBy> groupBys = query.getGroupBys();
            orderBy = groupBys;
        }
        if (orderBy == null || orderBy.isEmpty()) {
            orderBy = ExecUtils.getOrderBy(indexMeta);
        }
        return ExecUtils.copyOrderBys(orderBy);
    }

    public static List<IOrderBy> buildOrderBy(IDataNodeExecutor executor, IndexMeta indexMeta, String tableName) {
        IQuery query = ((IQuery) executor);
        List<IOrderBy> orderBy = null;
        orderBy = query.getOrderBys();
        if (orderBy == null || orderBy.isEmpty()) {
            List<IOrderBy> groupBys = query.getGroupBys();
            orderBy = groupBys;
        }
        if (orderBy == null || orderBy.isEmpty()) {
            orderBy = ExecUtils.getOrderBy(indexMeta);
        }
        List<IOrderBy> orderbys = ExecUtils.copyOrderBys(orderBy);
        // TODO: shenxun 这里是否应该去掉？
        // if(tableName != null && !tableName.isEmpty())
        // {
        // for(IOrderBy orderby : orderbys)
        // {
        // orderby.getColumn().setTableName(tableName);
        // }
        // }
        return orderbys;

    }

    public static My_JdbcHandler getJdbcHandler(DataSourceGetter dsGetter, IDataNodeExecutor executor,
                                                ExecutionContext executionContext) {
        DataSource ds;
        My_JdbcHandler jdbcHandler = new My_JdbcHandler(executionContext);

        ds = dsGetter.getDataSource(executionContext, executor.getDataNode());
        My_Transaction my_transaction = null;
        Transaction txn = executionContext.getTransaction();
        if (txn != null) {
            if (txn instanceof My_Transaction) {
                my_transaction = (My_Transaction) txn;
            }
        } else {
            my_transaction = new My_Transaction();
            executionContext.setTransaction(my_transaction);
        }
        jdbcHandler.setStrongConsistent(true);
        jdbcHandler.setDs(ds);
        jdbcHandler.setGroupName(executor.getDataNode());
        jdbcHandler.setStrongConsistent(false);
        jdbcHandler.setMyTransaction(my_transaction);
        jdbcHandler.setExtraCmd(context);
        jdbcHandler.setPlan(executor);
        return jdbcHandler;
    }

    protected static final List<IOrderBy> getOrderBy(List columns) {
        if (columns == null) {
            columns = Collections.EMPTY_LIST;
        }
        List<IOrderBy> orderBys = new ArrayList<IOrderBy>(columns.size());
        for (Object cobj : columns) {
            IColumn c = ExecUtils.getColumn(cobj);
            orderBys.add(ASTNodeFactory.getInstance().createOrderBy().setColumn(c).setDirection(true));
        }
        return orderBys;
    }
}
