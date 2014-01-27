package com.taobao.tddl.rule;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.model.lifecycle.Lifecycle;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;
import com.taobao.tddl.config.impl.UnitConfigDataHandlerFactory;
import com.taobao.tddl.monitor.logger.LoggerInit;
import com.taobao.tddl.rule.config.RuleChangeListener;
import com.taobao.tddl.rule.exceptions.TddlRuleException;
import com.taobao.tddl.rule.utils.StringXmlApplicationContext;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * tddl rule config管理
 * 
 * @author jianghang 2013-11-5 下午3:34:36
 * @since 5.0.0
 */
public class TddlRuleConfig extends AbstractLifecycle implements Lifecycle {

    protected static final Logger                               logger                       = LoggerFactory.getLogger(TddlRuleConfig.class);
    private static final int                                    TIMEOUT                      = 10 * 1000;
    private static final String                                 ROOT_BEAN_NAME               = "vtabroot";
    private static final String                                 TDDL_RULE_LE_PREFIX          = "com.taobao.tddl.rule.le.";
    private static final String                                 TDDL_RULE_LE_VERSIONS_FORMAT = "com.taobao.tddl.rule.le.{0}.versions";
    private static final String                                 NO_VERSION_NAME              = "__VN__";

    private String                                              appName;
    private String                                              unitName;

    // 本地规则
    private String                                              appRuleFile;
    private String                                              appRuleString;

    // 多套规则(动态推)
    private volatile ConfigDataHandlerFactory                   cdhf;
    private volatile ConfigDataHandler                          versionHandler;
    private volatile Map<String, ConfigDataHandler>             ruleHandlers                 = Maps.newHashMap();
    private volatile List<RuleChangeListener>                   listeners                    = Lists.newArrayList();

    /**
     * key = 0(old),1(new),2,3,4... value= version
     */
    private volatile Map<String, VirtualTableRoot>              vtrs                         = Maps.newLinkedHashMap();
    private volatile Map<String, String>                        ruleStrs                     = Maps.newHashMap();
    private volatile Map<Integer, String>                       versionIndex                 = Maps.newHashMap();
    private volatile Map<String, AbstractXmlApplicationContext> oldCtxs                      = Maps.newHashMap();

    private ClassLoader                                         outerClassLoader             = null;
    // 是否兼容历史老的rule，主要是tdd5代码修改过类的全路径，针对tddl3之前的rule需要考虑做兼容处理
    private boolean                                             compatibleOldRule            = true;

    public void doInit() {
        if (appRuleFile != null) { // 如果存在本地规则
            String[] rulePaths = appRuleFile.split(";");
            if (rulePaths.length == 1 && !rulePaths[0].matches("^V[0-9]*#.+$")) {
                // 本地文件单版本规则
                ApplicationContext ctx = buildRuleByFile(NO_VERSION_NAME, appRuleFile);
                vtrs.put(NO_VERSION_NAME, (VirtualTableRoot) ctx.getBean(ROOT_BEAN_NAME));
            } else {
                // 本地文件存在多版本规则
                // 一种文件配置写法： V0#classpath:xxx-rule.xml
                for (int i = 0; i < rulePaths.length; i++) {
                    if (rulePaths[i].matches("^V[0-9]*#.+$")) {
                        continue;
                    } else {
                        throw new TddlRuleException("rule file path \"" + rulePaths[i]
                                                    + " \" does not fit the pattern!");
                    }
                }
                for (int i = 0; i < rulePaths.length; i++) {
                    String rulePath = rulePaths[i];
                    String[] temp = rulePath.split("#");
                    ApplicationContext ctx = buildRuleByFile(temp[0], temp[1]);
                    vtrs.put(temp[0], (VirtualTableRoot) ctx.getBean(ROOT_BEAN_NAME));
                }
            }
        } else if (appRuleString != null) { // 直接设置了规则字符串
            ApplicationContext ctx = buildRuleByStr(NO_VERSION_NAME, appRuleString);
            vtrs.put(NO_VERSION_NAME, (VirtualTableRoot) ctx.getBean(ROOT_BEAN_NAME));
        } else if (appName != null) { // 使用动态rule配置
            String versionsDataId = getVersionsDataId(appName);
            if (cdhf == null) {
                cdhf = new UnitConfigDataHandlerFactory(unitName, appName);
            }
            versionHandler = cdhf.getConfigDataHandler(versionsDataId, new VersionsConfigListener());
            String versionData = versionHandler.getData(TIMEOUT, ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);
            if (versionData == null) { // fallback处理一下，无版本的rule
                String dataId = getNonversionedRuledataId(versionData);
                if (!dataSub(dataId, NO_VERSION_NAME, new SingleRuleConfigListener())) {
                    throw new TddlRuleException("subscribe the rule data or init rule error!check the error log!");
                }
            } else {
                String[] versions = versionData.split(",");
                for (String version : versions) {
                    String dataId = getVersionedRuleDataId(appName, version);
                    if (!dataSub(dataId, version, new SingleRuleConfigListener())) {
                        throw new RuntimeException("subscribe the rule data or init rule error!check the error log! the rule version is:"
                                                   + version);
                    }
                }

                // 记下日志,方便分析
                logRecieveRuleVersions(versionData);
            }
        }

        // 构建versionIndex
        int index = 0;
        Map<Integer, String> tempIndexMap = new HashMap<Integer, String>();
        for (String version : vtrs.keySet()) {
            tempIndexMap.put(index, version);
            index++;
        }
        this.versionIndex = tempIndexMap;
    }

    /**
     * <pre>
     * 返回当前使用的rule规则
     * 1. 如果是本地文件，则直接返回本地文件的版本 (本地文件存在多版本时，直接返回第一个版本)
     * 2. 如果是动态规则，则直接返回第一个版本
     * 
     * ps. 正常情况，只有一个版本会处于使用中，也就是在数据库动态切换出现多版本使用中.
     * </pre>
     */
    public VirtualTableRoot getCurrentRule() {
        if (versionIndex.size() == 0) {
            throw new TddlRuleException("规则对象为空!请检查是否存在规则!");
        }

        return vtrs.get(versionIndex.get(0));
    }

    public VirtualTableRoot getVersionRule(String version) {
        VirtualTableRoot vtr = vtrs.get(version);
        if (vtr == null) {
            throw new TddlRuleException("规则对象为空!请检查是否存在规则!");
        }

        return vtr;
    }

    /**
     * 获取当前在用的版本，理论上正常只有一个版本(切换时出现两个版本)，顺序返回版本，第一个版本为当前正在使用中的旧版本
     */
    public List<String> getAllVersions() {
        int size = versionIndex.size();
        List<String> versions = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            versions.add(versionIndex.get(i));
        }

        return versions;
    }

    /**
     * 初始化某个版本的rule
     */
    private synchronized boolean initVersionRule(String data, String version) {
        if (version == null) {
            version = NO_VERSION_NAME;
        }

        ApplicationContext ctx = null;
        try {
            // this rule may be wrong rule,don't throw it but log it,
            // and will not change the vtr!
            ctx = buildRuleByStr(version, data);
        } catch (Exception e) {
            logger.error("init rule error,rule str is:" + data, e);
            return false;
        }

        VirtualTableRoot tempvtr = (VirtualTableRoot) ctx.getBean(ROOT_BEAN_NAME);
        if (tempvtr != null) {
            // 直接覆盖
            vtrs.put(version, tempvtr);
            ruleStrs.put(version, data);
            AbstractXmlApplicationContext oldCtx = this.oldCtxs.get(version);
            // 销毁旧有容器
            if (oldCtx != null) {
                oldCtx.close();
            }
            // 记录一下当前ctx
            this.oldCtxs.remove(version);
            this.oldCtxs.put(version, (AbstractXmlApplicationContext) ctx);
        } else {
            logger.error("rule no vtabroot!!");
            return false;
        }
        return true;
    }

    /**
     * 尝试订阅一下
     * 
     * @throws TddlException
     */
    private boolean dataSub(String dataId, String version, ConfigDataListener listener) {
        ConfigDataHandler ruleHandler = cdhf.getConfigDataHandler(dataId, listener);
        try {
            String data = ruleHandler.getData(TIMEOUT, ConfigDataHandler.FIRST_CACHE_THEN_SERVER_STRATEGY);
            if (data == null) {
                logger.error("use diamond rule config,but recieve no config at all!");
                return false;
            }

            if (initVersionRule(data, version)) {
                this.ruleHandlers.put(version, ruleHandler);
                return true;
            }
        } catch (Exception e) {
            try {
                ruleHandler.destory();
            } catch (TddlException e1) {
                logger.error("destory failed!", e);
            }

            logger.error("get diamond data error!", e);
        }

        return false;
    }

    /**
     * remove listeners
     * 
     * @throws TddlException
     */
    public void doDestory() throws TddlException {
        if (versionHandler != null) {
            versionHandler.destory();
        }

        for (ConfigDataHandler ruleListener : this.ruleHandlers.values()) {
            ruleListener.destory();
        }
    }

    // ======================= help metod ==================

    /**
     * 基于文件创建rule的spring容器
     * 
     * @param file
     * @return
     */
    private ApplicationContext buildRuleByFile(String version, String file) {
        try {
            Resource resource = new PathMatchingResourcePatternResolver().getResource(file);
            String ruleStr = StringUtils.join(IOUtils.readLines(resource.getInputStream()), SystemUtils.LINE_SEPARATOR);
            return buildRuleByStr(version, ruleStr);
        } catch (IOException e) {
            throw new TddlRuleException(e);
        }
    }

    /**
     * 基于string字符流创建rule的spring容器
     * 
     * @param data
     * @return
     */
    private ApplicationContext buildRuleByStr(String version, String data) {
        if (compatibleOldRule) {
            data = RuleCompatibleHelper.compatibleRule(data);
        }
        ApplicationContext applicationContext = new StringXmlApplicationContext(data, outerClassLoader);
        ruleStrs.put(version, data); // 记录一下
        return applicationContext;
    }

    private String getCurrentRuleStr() {
        if (this.ruleStrs != null && this.ruleStrs.size() > 0) {
            String ruleStr = this.ruleStrs.get(versionIndex.get(0));
            return ruleStr;
        } else {
            throw new TddlRuleException("规则对象为空!请检查diamond上是否存在动态规则!");
        }
    }

    public void logRecieveRuleVersions(String version) {
        if (LoggerInit.DYNAMIC_RULE_LOG.isInfoEnabled()) {
            SimpleDateFormat df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss:SSS");
            String logFieldSep = "#@#";
            String linesep = System.getProperty("line.separator");
            String time = df.format(new Date());
            StringBuilder sb = new StringBuilder().append(appName)
                .append(logFieldSep)
                .append(version)
                .append(logFieldSep)
                .append(time)
                .append(logFieldSep)
                .append(1)
                .append(linesep);

            LoggerInit.DYNAMIC_RULE_LOG.info(sb.toString());
        }
    }

    private void logReceiveRule(String dataId, String data) {
        StringBuilder sb = new StringBuilder("recieve versions data!dataId:");
        sb.append(dataId);
        sb.append(" data:");
        sb.append(data);
        logger.info(sb.toString());
    }

    /**
     * 获取appname的versions列表的dataId
     * 
     * @param appName
     * @return
     */
    public static String getVersionsDataId(String appName) {
        String versionsDataId = new MessageFormat(TDDL_RULE_LE_VERSIONS_FORMAT).format(new Object[] { appName });
        return versionsDataId;
    }

    /**
     * 获取appname指定version的dataId
     * 
     * @param appName
     * @param version
     * @return
     */
    public static String getVersionedRuleDataId(String appName, String version) {
        return TDDL_RULE_LE_PREFIX + appName + "." + version;
    }

    /**
     * 获取appname无版本信息的dataId
     * 
     * @param appName
     * @return
     */
    public static String getNonversionedRuledataId(String appName) {
        return TDDL_RULE_LE_PREFIX + appName;
    }

    // ================== listener =======================

    private class VersionsConfigListener implements ConfigDataListener {

        public synchronized void onDataRecieved(String dataId, String data) {
            if (TStringUtil.isNotEmpty(data)) {
                String[] versions = data.split(",");
                Map<String, String> checkMap = new HashMap<String, String>();
                // 添加新增的规则订阅
                int index = 0;
                Map<Integer, String> tempIndexMap = new HashMap<Integer, String>();
                for (String version : versions) {
                    if (ruleHandlers.get(version) == null) {
                        String ruleDataId = getVersionedRuleDataId(appName, version);
                        if (!dataSub(ruleDataId, version, new SingleRuleConfigListener())) {
                            return;
                        }
                    }
                    checkMap.put(version, version);
                    tempIndexMap.put(index, version);
                    index++;
                }

                versionIndex = tempIndexMap;

                // 删除没有在version中存在的订阅
                List<String> needRemove = new ArrayList<String>();
                for (Map.Entry<String, ConfigDataHandler> handler : ruleHandlers.entrySet()) {
                    if (checkMap.get(handler.getKey()) == null) {
                        needRemove.add(handler.getKey());
                    }
                }

                // 清理
                for (String version : needRemove) {
                    ConfigDataHandler handler = ruleHandlers.get(version);
                    try {
                        handler.destory();
                    } catch (TddlException e) {
                        logger.error("destory failed!", e);
                    }
                    ruleHandlers.remove(version);
                    vtrs.remove(version);
                    ruleStrs.remove(version);
                    oldCtxs.get(version).close();
                    oldCtxs.remove(version);
                }

                // 在versions data收到为null,或者为空,不调用,保护AppServer
                // 调用listener,但只返回位列第一个的VirtualTableRoot
                for (RuleChangeListener listener : listeners) {
                    try {
                        // may be wrong,so try catch it ,not to affect
                        // other!
                        listener.onRuleRecieve(getCurrentRuleStr());
                    } catch (Exception e) {
                        logger.error("one listener error!", e);
                    }
                }

            }

            // 记下日志,方便分析
            logRecieveRuleVersions(data);
        }

    }

    private class SingleRuleConfigListener implements ConfigDataListener {

        public synchronized void onDataRecieved(String dataId, String data) {
            if (TStringUtil.isNotEmpty(data)) {
                logReceiveRule(dataId, data);
                String prefix = TDDL_RULE_LE_PREFIX + appName + ".";
                int i = dataId.indexOf(prefix);
                String version = NO_VERSION_NAME; // non-versioned rule
                if (i >= 0) {
                    version = dataId.substring(i + prefix.length());
                }

                if (initVersionRule(data, version)) {
                    for (RuleChangeListener listener : listeners) {
                        try {
                            // may be wrong,so try catch it ,not to
                            // affect other !
                            listener.onRuleRecieve(getCurrentRuleStr());
                        } catch (Exception e) {
                            logger.error("one listener error!", e);
                        }
                    }
                }
            }
        }
    }

    // =================== setter / getter ======================

    public void setOuterClassLoader(ClassLoader outerClassLoader) {
        this.outerClassLoader = outerClassLoader;
    }

    public void addRuleChangeListener(RuleChangeListener listener) {
        this.listeners.add(listener);
    }

    public void setAppRuleFile(String appRuleFile) {
        this.appRuleFile = appRuleFile;
    }

    public void setAppRuleString(String appRuleString) {
        this.appRuleString = appRuleString;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public void setCompatibleOldRule(boolean compatibleOldRule) {
        this.compatibleOldRule = compatibleOldRule;
    }

}
