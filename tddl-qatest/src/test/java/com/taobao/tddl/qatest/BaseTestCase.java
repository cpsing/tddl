package com.taobao.tddl.qatest;

import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import com.taobao.diamond.mockserver.MockServer;
import com.taobao.tddl.matrix.jdbc.TDataSource;
import com.taobao.tddl.qatest.util.DateUtil;
import com.taobao.tddl.qatest.util.Validator;

/**
 * 基本测试类，提供创建AndorServer和AndorClient方法
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2012-2-16 下午2:05:24
 */
@SuppressWarnings("rawtypes")
@Ignore(value = "提供初始化环境的实际方法")
public class BaseTestCase extends Validator {

    // --------------公用变量
    protected static final long       RANDOM_ID     = Long.valueOf(RandomStringUtils.randomNumeric(8));
    protected final int               MAX_DATA_SIZE = 20;
    protected final int               RANDOM_INT    = Integer.valueOf(RandomStringUtils.randomNumeric(8));
    protected final float             fl            = 0.01f;
    protected Random                  rand          = new Random();
    protected final String            name          = "zhuoxue";
    protected final String            name1         = "zhuoxue_yll";
    protected final String            name2         = "zhuoxue%yll";
    protected final String            newName       = "zhuoxue.yll";
    protected final String            school        = "taobao";
    protected final Date              gmtDay        = new Date(1350230400000l);
    protected final Date              gmtDayBefore  = new Date(1150300800000l);
    protected final Date              gmtDayNext    = new Date(1550246400000l);
    protected final Date              gmt           = new Date(1350304585380l);
    protected final Date              gmtNext       = new Date(1550304585380l);
    protected final Date              gmtBefore     = new Date(1150304585380l);
    // --------------公用表名
    protected static String           normaltblTableName;
    protected static String           studentTableName;
    protected static String           host_info;
    protected static String           hostgroup;
    protected static String           hostgroup_info;
    protected static String           module_info;
    protected static String           module_host;
    // datasource为static，一个测试类只启动一次
    protected static TDataSource      us;
    protected ResultSet               rc            = null;
    protected ResultSet               rs            = null;

    protected final NumberFormat      nf            = new DecimalFormat("#.#");
    protected String                  timeString    = DateUtil.formatDate(new Date(0), DateUtil.DATE_FULLHYPHEN);
    protected static final Logger     logger        = Logger.getLogger(BaseTestCase.class);
    protected static final Properties properties    = new Properties();

    @BeforeClass
    public static void diamondSetUp() {
        MockServer.setUpMockServer();
    }

    @AfterClass
    public static void diamondTearDown() {
        MockServer.tearDownMockServer();
        // TAtomDataSource.cleanAllDataSource();
    }

}
