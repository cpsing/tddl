package com.taobao.tddl.qatest;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.common.model.ExtraCmd;
import com.taobao.tddl.config.ConfigServerHelper;
import com.taobao.tddl.group.jdbc.TGroupDataSource;
import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.qatest.util.LoadPropsUtil;
import com.taobao.tddl.qatest.util.PrepareData;

/**
 * 基本测试类
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-2-16 下午2:05:24
 */
public class BaseMatrixTestCase extends PrepareData {

    protected static final ExecutorService pool                    = Executors.newCachedThreadPool();
    private static String                  ruleFile                = "V0#classpath:matrix/";
    private static String                  rule                    = "rule.xml";

    private static String                  schemaFile              = "matrix/";
    private static String                  schema                  = "schema.xml";
    // dbType为mysql运行mysql测试，bdb值为bdb运行bdb测试，如果为空则运行bdb和mysql测试
    protected static String                dbType                  = null;

    protected static boolean               needPerparedData        = true;
    private static String                  machineTapologyFile     = "src/test/resources/matrix/server_topology.xml";
    private static String                  machineTapologyAyncFile = "src/test/resources/matrix/server_async_topology.xml";

    private static String                  typeFile                = "db_type.properties";

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
            } else if (dbType.equals("mysql") || dbType.equals("tdhs") || dbType.equals("tddl")
                       || dbType.equals("hbase")) {
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
        if ("tddl".equalsIgnoreCase(dbTypeStack) || "mysql".equalsIgnoreCase(dbTypeStack)) {
            us.setAppName("andor_mysql_qatest");
        } else if ("tdhs".equalsIgnoreCase(dbTypeStack)) {
            us.setAppName("andor_tdhs_qatest");
        } else if ("hbase".equalsIgnoreCase(dbTypeStack)) {
            us.setAppName("andor_hbase_qatest");
        }

        us.setRuleFile(ruleFile + dbTypeStack + "_" + rule);

        if ("tddl".equalsIgnoreCase(dbTypeStack)) {
        }
        if ((!"tddl".equalsIgnoreCase(dbTypeStack)) && (!"tdhs".equalsIgnoreCase(dbTypeStack))) {
            us.setTopologyFile(machineTapologyFile);
            us.setSchemaFile(schemaFile + dbTypeStack + "_" + schema);
        }

        Map<String, Comparable> cp = new HashMap<String, Comparable>();
        if ("tdhs".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.ConnectionExtraCmd.USE_TDHS_FOR_DEFAULT, "true");
        }
        if (async && "mysql".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.ExecutionExtraCmd.ALLOW_BIO, "FALSE");
            us.setTopologyFile(machineTapologyAyncFile);
        }

        if ("hbase".equalsIgnoreCase(dbTypeStack)) {
            cp.put(ExtraCmd.ExecutionExtraCmd.HBASE_MAPPING_FILE, "matrix/hbase_mapping.xml");
        }
        // cp.put(ExtraCmd.ConnectionExtraCmd.INIT_TEMP_TABLE, "true");
        // cp.put(ExtraCmd.ConnectionExtraCmd.INIT_TDDL_DATASOURCE, "true");
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

    protected static void setMatrixMockInfo(String groupsPath, String key) throws Exception {
        setMatrixMockInfo(groupsPath, key, false);
    }

    protected static void setMatrixMockInfo(String groupsPath, String key, boolean isOracle) throws Exception {
        // -----------------获取groups信息
        String groupsStr = LoadPropsUtil.loadProps2OneLine(groupsPath, key);
        if (groupsStr == null || StringUtils.isBlank(groupsStr)) {
            throw new Exception("指定path = " + groupsPath + ",key = " + key + "的groups信息为null或者为空字符。");
        }

        // -----------------oracle or mysql
        String groupPath = BaseAtomGroupTestCase.GROUP_PATH;
        String atomPath = BaseAtomGroupTestCase.ATOM_PATH;
        if (isOracle) {
            groupPath = BaseAtomGroupTestCase.GROUP_ORA_PATH;
            atomPath = BaseAtomGroupTestCase.ATOM_ORA_PATH;
        }

        // -----------------获取group信息
        BaseAtomGroupTestCase.dataMap = new HashMap<String, String>();
        String[] groupArr = groupsStr.split(",");

        for (String group : groupArr) {
            group = group.trim();
            String groupStr = "";
            groupStr = LoadPropsUtil.loadProps2OneLine(groupPath + group + BaseAtomGroupTestCase.PROPERTIES_FILE, group);
            if (groupsStr != null && StringUtils.isNotBlank(groupsStr)) {
                // 获取atom信息
                String[] atomArr = groupStr.split(",");
                for (String atom : atomArr) {
                    atom = atom.trim();
                    atom = atom.substring(0, atom.indexOf(":"));
                    BaseAtomGroupTestCase.initAtomConfig(atomPath + atom, BaseAtomGroupTestCase.APPNAME, atom);
                }
                // 获取groupkey
                BaseAtomGroupTestCase.dataMap.put(TGroupDataSource.getFullDbGroupKey(group), groupStr);
            }
        }

        // -----------------dbgroups
        BaseAtomGroupTestCase.dataMap.put(new MessageFormat(ConfigServerHelper.DATA_ID_DB_GROUP_KEYS).format(new Object[] { BaseAtomGroupTestCase.APPNAME }),
            groupsStr);

        // 建立MockServer
        MockServer.setConfigInfos(BaseAtomGroupTestCase.dataMap);
    }
}
