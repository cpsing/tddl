package com.taobao.tddl.group;

import java.sql.Connection;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.alibaba.druid.pool.DruidDataSource;
import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.atom.common.TAtomConstants;
import com.taobao.tddl.atom.config.TAtomConfParser;
import com.taobao.tddl.atom.config.TAtomDsConfDO;
import com.taobao.tddl.common.model.DBType;
import com.taobao.tddl.group.jdbc.DataSourceWrapper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.group.utils.PropLoadTestUtil;

@Ignore("测试基类")
public class BaseGroupTest {

    protected static final String APPNAME = "tddl_sample";
    protected static final String GROUP0  = "tddl_sample_group_0";
    protected static final String GROUP1  = "tddl_sample_group_1";
    protected static final String GROUP2  = "tddl_sample_group_2";
    protected static final String DSKEY0  = "tddl_sample_0";
    protected static final String DSKEY1  = "tddl_sample_1";
    protected static final String DSKEY2  = "tddl_sample_2";

    @BeforeClass
    public static void beforeClass() {
        MockServer.setUpMockServer();
        // 初始化一些配置
        mockConfig("group0", APPNAME, GROUP0, DSKEY0);
        mockConfig("group1", APPNAME, GROUP1, DSKEY1);
        mockConfig("group2", APPNAME, GROUP2, DSKEY2);
    }

    @AfterClass
    public static void after() {
        MockServer.tearDownMockServer();
    }

    @Before
    public void setUp() throws Exception {
        deleteAll();
    }

    private static void mockConfig(String dir, String appName, String groupName, String dbKey) {
        String globaStr = PropLoadTestUtil.loadPropFile2String("conf/" + dir + "/globa.properties");
        MockServer.setConfigInfo(TAtomConstants.getGlobalDataId(dbKey), globaStr);

        String appStr = PropLoadTestUtil.loadPropFile2String("conf/" + dir + "/app.properties");
        MockServer.setConfigInfo(TAtomConstants.getAppDataId(appName, dbKey), appStr);

        String passwdStr = PropLoadTestUtil.loadPropFile2String("conf/" + dir + "/passwd.properties");
        // 解析配置
        TAtomDsConfDO tAtomDsConfDO = TAtomConfParser.parserTAtomDsConfDO(globaStr, appStr);
        String passwdDataId = TAtomConstants.getPasswdDataId(tAtomDsConfDO.getDbName(),
            tAtomDsConfDO.getDbType(),
            tAtomDsConfDO.getUserName());
        MockServer.setConfigInfo(passwdDataId, passwdStr);
    }

    public static DataSource getMySQLDataSource() {
        return getMySQLDataSource(0);
    }

    public static DataSource getMySQLDataSource(int num) {
        if (num > 2) {
            num = 2;
        }
        DruidDataSource ds = new DruidDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUsername("tddl");
        ds.setPassword("tddl");
        ds.setUrl("jdbc:mysql://10.232.31.154/tddl_sample_" + num);
        return ds;

    }

    // 删除三个库中crud表的所有记录
    public static void deleteAll() throws Exception {
        DataSource ds1 = getMySQLDataSource(0);
        DataSource ds2 = getMySQLDataSource(1);
        DataSource ds3 = getMySQLDataSource(2);

        Connection conn = null;
        Statement stmt = null;

        TGroupDataSource ds = new TGroupDataSource();
        DataSourceWrapper dsw = new DataSourceWrapper("tddl_sample_0", "rw", ds1, DBType.MYSQL);
        ds.init(dsw);

        conn = ds.getConnection();
        stmt = conn.createStatement();
        stmt.executeUpdate("delete from tddl_test_0000");
        stmt.close();
        conn.close();

        ds = new TGroupDataSource();
        dsw = new DataSourceWrapper("tddl_sample_1", "rw", ds2, DBType.MYSQL);
        ds.init(dsw);
        conn = ds.getConnection();
        stmt = conn.createStatement();
        stmt.executeUpdate("delete from tddl_test_0000");
        stmt.close();
        conn.close();

        ds = new TGroupDataSource();
        dsw = new DataSourceWrapper("tddl_sample_2", "rw", ds3, DBType.MYSQL);
        ds.init(dsw);
        conn = ds.getConnection();
        stmt = conn.createStatement();
        stmt.executeUpdate("delete from tddl_test_0000");
        stmt.close();
        conn.close();
    }
}
