package com.taobao.tddl.qatest;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.qatest.util.LoadPropsUtil;
import com.taobao.tddl.qatest.util.PrepareData;

/**
 * 基本测试类
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-2-16 下午2:05:24
 */
@Ignore(value = "提供初始化环境的实际方法")
public class BaseMatrixTestCase extends PrepareData {

    protected static final ExecutorService pool                = Executors.newCachedThreadPool();
    private static String                  ruleFile            = "V0#classpath:matrix/";
    private static String                  rule                = "rule.xml";

    private static String                  schemaFile          = "matrix/";
    private static String                  schema              = "schema.xml";
    // dbType为mysql运行mysql测试，bdb值为bdb运行bdb测试，如果为空则运行bdb和mysql测试
    protected static String                dbType              = null;

    protected static boolean               needPerparedData    = true;
    private static String                  machineTapologyFile = "matrix/server_topology.xml";

    private static String                  typeFile            = "db_type.properties";

    static {
        dbType = LoadPropsUtil.loadProps(typeFile).getProperty("dbType");
    }

    @BeforeClass
    public static void IEnvInit() throws Exception {
        MockServer.tearDownMockServer();
        // setMatrixMockInfo(MATRIX_DBGROUPS_PATH, TDDL_DBGROUPS);

        if (us == null) {
            if (dbType.equals("bdb") || dbType == "") {
                JDBCClient(dbType);
            } else if (dbType.equals("mysql") || dbType.equals("tdhs") || dbType.equals("hbase")) {
                JDBCClient(dbType, false);
            }
        }
    }

    @Before
    public void prepareConnection() throws SQLException {
        con = getConnection();
        andorCon = us.getConnection();
    }

    @After
    public void clearDate() throws Exception {
        psConRcRsClose(rc, rs);
    }

    public static void JDBCClient(String dbType) throws Exception {
        JDBCClient(dbType, false);
    }

    public static void JDBCClient(String dbTypeStack, boolean async) throws Exception {
        us = new TDataSource();
        // if ("tddl".equalsIgnoreCase(dbTypeStack) ||
        // "mysql".equalsIgnoreCase(dbTypeStack)) {
        us.setAppName("andor_mysql_qatest");
        // } else if ("tdhs".equalsIgnoreCase(dbTypeStack)) {
        // us.setAppName("andor_tdhs_qatest");
        // } else if ("hbase".equalsIgnoreCase(dbTypeStack)) {
        // us.setAppName("andor_hbase_qatest");
        // }

        us.setRuleFile(ruleFile + dbTypeStack + "_" + rule);

        if ((!"tddl".equalsIgnoreCase(dbTypeStack)) && (!"tdhs".equalsIgnoreCase(dbTypeStack))) {
            us.setTopologyFile(machineTapologyFile);
            us.setSchemaFile(schemaFile + dbTypeStack + "_" + schema);
        }

        Map<String, Object> cp = new HashMap<String, Object>();
        if ("tdhs".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.USE_TDHS_FOR_DEFAULT, "true");
        }

        if ("hbase".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.HBASE_MAPPING_FILE, "matrix/hbase_mapping.xml");
        }
        us.setConnectionProperties(cp);
        try {
            us.init();
        } catch (Exception e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }
    }

    public static JdbcTemplate JdbcTemplateClient(String dbType) throws Exception {
        IEnvInit();
        return new JdbcTemplate(us);
    }

}
