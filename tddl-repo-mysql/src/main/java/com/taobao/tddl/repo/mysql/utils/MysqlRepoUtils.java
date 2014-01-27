package com.taobao.tddl.repo.mysql.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.spi.IDataSourceGetter;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.config.table.IndexMeta;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.core.plan.query.IQuery;
import com.taobao.tddl.repo.mysql.spi.My_JdbcHandler;
import com.taobao.tddl.repo.mysql.spi.My_Transaction;

/**
 * @author mengshi.sunmengshi 2013-12-5 下午6:08:32
 * @since 5.0.0
 */
public class MysqlRepoUtils {

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

        return orderbys;

    }

    public static My_JdbcHandler getJdbcHandler(IDataSourceGetter dsGetter, IDataNodeExecutor executor,
                                                ExecutionContext executionContext) {
        DataSource ds;
        My_JdbcHandler jdbcHandler = new My_JdbcHandler(executionContext);

        ds = dsGetter.getDataSource(executor.getDataNode());
        My_Transaction my_transaction = null;
        ITransaction txn = executionContext.getTransaction();
        if (txn != null) {
            if (txn instanceof My_Transaction) {
                my_transaction = (My_Transaction) txn;
            }
        } else {
            my_transaction = new My_Transaction(executionContext.isAutoCommit());
            executionContext.setTransaction(my_transaction);
        }
        jdbcHandler.setDs(ds);
        jdbcHandler.setGroupName(executor.getDataNode());
        jdbcHandler.setMyTransaction(my_transaction);
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
