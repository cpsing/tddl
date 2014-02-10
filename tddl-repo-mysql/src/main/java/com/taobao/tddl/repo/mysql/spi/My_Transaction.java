package com.taobao.tddl.repo.mysql.spi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.executor.common.AtomicNumberCreator;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.group.jdbc.TGroupConnection;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class My_Transaction implements ITransaction {

    protected final static Logger           logger                = LoggerFactory.getLogger(My_Transaction.class);
    private AtomicNumberCreator             idGen                 = AtomicNumberCreator.getNewInstance();
    private Integer                         id                    = idGen.getIntegerNextNumber();

    /**
     * 处于事务中的连接管理
     */
    protected Map<String, List<Connection>> connMap               = new HashMap<String, List<Connection>>(1);

    /**
     * 当前进行事务的节点
     */
    protected String                        transactionalNodeName = null;
    protected boolean                       autoCommit            = true;
    protected Stragety                      stragety              = Stragety.STRONG;

    public enum Stragety {

        /** 跨机允许读不允许写 */
        ALLOW_READ,
        /** 跨机读写都不允许 */
        STRONG,
        /** 随意跨机 */
        NONE
    }

    public My_Transaction(boolean autoCommit){
        this.autoCommit = autoCommit;
    }

    public void beginTransaction() {
        if (connMap != null && !connMap.isEmpty()) {
            try {
                if (connMap != null && !connMap.isEmpty()) {
                    for (List<Connection> conns : connMap.values()) {
                        for (Connection conn : conns) {
                            conn.setAutoCommit(false);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 策略两种：1. 强一致策略，事务中不允许跨机查询。2.弱一致策略，事务中允许跨机查询；
     * 
     * @param groupName
     * @param ds
     * @param strongConsistent 这个请求是否是强一致的，这个与ALLOW_READ一起作用。
     * 当ALLOW_READ的情况下，strongConsistent =
     * true时，会创建事务链接，而如果sConsistent=false则会创建非事务链接
     * @return
     */
    public Connection getConnection(String groupName, DataSource ds) throws SQLException {
        if (groupName == null) {
            throw new IllegalArgumentException("group name is null");
        }

        if (autoCommit) {// 自动提交，不建立事务链接
            return newConnection(ds);
        }

        // if (stragety == Stragety.NONE) {
        // Connection my_JdbcHandler = getConnection(groupName, ds, true);
        // return my_JdbcHandler;
        // } else if (stragety == Stragety.ALLOW_READ) {
        // if (!strongConsistent &&
        // !groupName.equalsIgnoreCase(transactionalNodeName)) {// 非强一致，又非事务用链接
        // Connection my_JdbcHandler = getConnection(groupName, ds, true);
        // return my_JdbcHandler;
        // }
        // }

        /*
         * 状态是强一致或ALLOW_READ 策略一致
         */
        if (transactionalNodeName != null) {// 已经有事务链接了
            if (transactionalNodeName.equalsIgnoreCase(groupName)) {
                List<Connection> conn = getConnections(transactionalNodeName, ds);
                if (conn.size() != 1 && conn.get(0).getAutoCommit()) {
                    // 拿出来的应该是已经存在的链接，这个链接也必然是事务链接
                    throw new RuntimeException("connection is not transactional? should not be here");
                }
                return conn.get(0);
            } else {
                throw new RuntimeException("只支持单机事务，当前进行事务的是" + transactionalNodeName + " . 你现在希望进行操作的db是：" + groupName);
            }
        } else {// 没有事务建立，新建事务
            transactionalNodeName = groupName;
            Connection handler = getConnection(groupName, ds);
            return handler;
        }
    }

    private List<Connection> getConnections(String groupName, DataSource ds) throws SQLException {
        List<Connection> conns = connMap.get(groupName);
        if (conns == null || conns.isEmpty()) {
            conns = new ArrayList();
            Connection conn = newConnection(ds);
            conns.add(conn);
            connMap.put(groupName, conns);
        }

        if (!autoCommit) {
            for (Connection conn : conns) {
                conn.setAutoCommit(false);
            }
        }
        return conns;
    }

    private Connection newConnection(DataSource ds) throws SQLException {
        Connection myConn = ds.getConnection();
        return myConn;
    }

    public void commit() throws TddlException {
        try {
            if (connMap != null && !connMap.isEmpty()) {
                for (List<Connection> conns : connMap.values()) {
                    for (Connection conn : conns) {
                        conn.commit();
                    }
                }
            }
        } catch (SQLException e) {
            throw new TddlException(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, e);
        }

        transactionalNodeName = null;
    }

    public void rollback() throws TddlException {
        try {
            if (connMap != null && !connMap.isEmpty()) {
                for (List<Connection> conns : connMap.values()) {
                    for (Connection conn : conns) {
                        conn.rollback();
                    }
                }
            }
        } catch (SQLException e) {
            throw new TddlException(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, e);
        }
        transactionalNodeName = null;
    }

    public long getId() {
        return id;
    }

    public ITHLog getHistoryLog() {
        return null;
    }

    public void close() throws TddlException {
        if (autoCommit) {
            // 如果是auto commit模式，因为不存在重用，链接关闭自管理
            return;
        }

        SQLException exception = null;
        if (connMap != null && !connMap.isEmpty()) {
            for (List<Connection> conns : connMap.values()) {
                for (Connection conn : conns) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        logger.error("", e);
                        exception = e;
                    }
                }
            }
            connMap.clear();
        }
        if (exception != null) {
            throw new TddlException(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, exception);
        }
    }

    public static void closeStreaming(My_Transaction trans, String groupName, DataSource ds) throws SQLException {
        List<Connection> conns = trans.getConnections(groupName, ds);
        for (Connection con : conns) {
            closeStreaming(con);
        }

    }

    public static void closeStreaming(Connection con) throws SQLException {
        TGroupConnection myconn = getTGroupConnection(con);
        // Jboss包装
        // MySQL包装
        // 获取当前链接执行的ID
        Long thdid = myconn.getId();

        if (thdid == -1) {
            myconn.close();
            return;
        }
        // 这里是新建一个链接来关闭，也可以用连接池里的，不过可能会造成额外等待。。所以还是抄驱动的方式吧。
        // 复制一个链接(等于新创建一个链接)
        Connection conNew = null;

        try {
            conNew = myconn.duplicate();
            // 使用这个链接关闭对应的thdid,主要是为了让ServerKillProcess..从而可以丢流异常，而非drain所有数据到本地
            conNew.createStatement().executeUpdate("KILL QUERY " + thdid);
        } finally {
            conNew.close();
            // 将mysql真正的链接关闭掉。
            myconn.close();
        }
        // 关闭新建立的这个链接

        /**
         * 这以后，主要是为了让Jboss能够知道当前这个链接已经挂了。。。被关闭掉了
         */
        // 执行一条SQL，让他抛出链接已经被关闭的异常。
        try {
            PreparedStatement ps = con.prepareStatement("select 1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            rs.close();
        } catch (Exception e) {
            logger.debug(e);
        }
    }

    private static TGroupConnection getTGroupConnection(Connection con) {
        if (con instanceof TGroupConnection) {
            return (TGroupConnection) con;
        }

        throw new RuntimeException("impossible,connection is not TGroupConnection:" + con.getClass());
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Map<String, List<Connection>> getConnMap() {
        return connMap;
    }

    public void setConnMap(Map<String, List<Connection>> connMap) {
        this.connMap = connMap;
    }

    public String getTransactionalNodeName() {
        return transactionalNodeName;
    }

    public void setTransactionalNodeName(String transactionalNodeName) {
        this.transactionalNodeName = transactionalNodeName;
    }

}
