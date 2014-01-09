package com.taobao.tddl.qatest.group;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.qatest.BaseAtomGroupTestCase;

/**
 * Comment for GroupEasyMock
 * <p/>
 * Created Date: 2010-12-11 09:53:35
 */
public class GroupEasyMock extends BaseAtomGroupTestCase {

    protected static TGroupDataSource  tds;
    protected static IMocksControl     ctl   = EasyMock.createControl();
    protected static DataSource        ds    = null;
    protected static DataSource        ds1   = null;
    protected static Connection        conn  = null;
    protected static Connection        conn1 = null;
    protected static PreparedStatement ps    = null;
    protected static PreparedStatement ps1   = null;
    protected static ResultSet         rs    = null;
    protected static ResultSet         rs1   = null;

    @BeforeClass
    public static void setUp() throws Exception {
        tds = new TGroupDataSource();
        tds.setAppName(APPNAME);
        tds.setDbGroupKey(GROUPKEY_0);

        ds = ctl.createMock(DataSource.class);
        ds1 = ctl.createMock(DataSource.class);

        DataSourceWrapper dsw1 = new DataSourceWrapper("k1", "wr", ds, DBType.MYSQL);
        DataSourceWrapper dsw2 = new DataSourceWrapper("k", "r", ds1, DBType.MYSQL);

        tds.init(dsw1, dsw2);
    }

    @AfterClass
    public static void tearDown() {
        ds = null;
        ds1 = null;
        ctl = null;
    }

    @Before
    public void init() throws Exception {
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        conn = ctl.createMock(Connection.class);
        ps = ctl.createMock(PreparedStatement.class);
        rs = ctl.createMock(ResultSet.class);

        conn1 = ctl.createMock(Connection.class);
        ps1 = ctl.createMock(PreparedStatement.class);
        rs1 = ctl.createMock(ResultSet.class);
    }

    @After
    public void destroy() throws Exception {
        if (EASYMOCK_SHOULD_NOT_BE_TEST) {
            return;
        }

        conn = null;
        ps = null;
        rs = null;

        conn1 = null;
        ps1 = null;
        rs1 = null;
    }
}
