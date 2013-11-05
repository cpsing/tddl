package com.taobao.tddl.monitor.logger.logback;

import java.io.File;
import java.nio.charset.Charset;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

import com.taobao.tddl.monitor.logger.DynamicLogger;
import com.taobao.tddl.monitor.logger.LoggerInit;

public class DynamicLogbackLogger extends DynamicLogger {

    public void init() {
        Appender tddlAppender = buildAppender("TDDL_Appender", "tddl.log", "%d %p [%c{10}] - %m%n");
        Appender md5sqlAppender = buildAppender("TDDL_MD5_TO_SQL_Appender", "tddl.md5sql.log", "%d %p [%c{10}] - %m%n");
        Appender nagiosAppender = buildAppender("TDDL_Nagios_Appender", "Nagios.log", "%m%n");
        Appender atomStatisticAppender = buildDailyMaxRollingAppender("TDDL_Atom_Statistic_Appender",
            "tddl-atom-statistic.log",
            "%m",
            6);
        Appender matrixStatisticAppender = buildDailyMaxRollingAppender("TDDL_Matrix_Statistic_Appender",
            "tddl-matrix-statistic.log",
            "%m",
            12);
        Appender connStatisticAppender = buildDailyMaxRollingAppender("TDDL_Conn_Statistic_Appender",
            "tddl-conn-statistic.log",
            "%m",
            6);

        Appender statisticAppender = buildAppender("TDDL_Statistic_Appender", "tddl-statistic.log", "%m");
        Appender snapshotAppender = buildAppender("TDDL_Snapshot_Appender", "tddl-snapshot.log", "%m");

        ch.qos.logback.classic.Logger logger = (Logger) LoggerInit.TDDL_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(tddlAppender);
        logger.setLevel(Level.WARN);

        logger = (Logger) LoggerInit.TDDL_MD5_TO_SQL_MAPPING.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(md5sqlAppender);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) LoggerInit.TDDL_Nagios_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(nagiosAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Atom_Statistic_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(atomStatisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Matrix_Statistic_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(matrixStatisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Conn_Statistic_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(connStatisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Statistic_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(statisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Snapshot_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(snapshotAppender);
        logger.setLevel(Level.INFO);
    }

    public void initRule() {
        Appender dbTabAppender = buildAppender("TDDL_Vtab_Appender", "tddl-db-tab.log", "%m");
        Appender vSlotAppender = buildAppender("TDDL_Vtab_Appender", "tddl-vslot.log", "%m");
        Appender dynamicRuleAppender = buildAppender("TDDL_DynamicRule_Appender", "tddl-dynamic-rule.log", "%m");

        ch.qos.logback.classic.Logger logger = (Logger) LoggerInit.TDDL_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(dbTabAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.VSLOT_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(vSlotAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.DYNAMIC_RULE_LOG.getDelegate();
        logger.setAdditive(false);
        logger.detachAndStopAllAppenders();
        logger.addAppender(dynamicRuleAppender);
        logger.setLevel(Level.INFO);
    }

    private Appender buildAppender(String name, String fileName, String pattern) {
        RollingFileAppender appender = new RollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());

        TimeBasedRollingPolicy rolling = new TimeBasedRollingPolicy();
        rolling.setParent(appender);
        rolling.setFileNamePattern(new File(getLogPath(), fileName).getAbsolutePath() + ".%d{yyyy-MM-dd}");
        appender.setRollingPolicy(rolling);

        PatternLayoutEncoder layout = new PatternLayoutEncoder();
        layout.setPattern(pattern);
        layout.setCharset(Charset.forName(getEncoding()));
        appender.setEncoder(layout);
        // 启动
        appender.start();
        return appender;
    }

    private Appender buildDailyMaxRollingAppender(String name, String fileName, String pattern, int maxBackupIndex) {
        RollingFileAppender appender = new RollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());

        TimeBasedRollingPolicy rolling = new TimeBasedRollingPolicy();
        rolling.setFileNamePattern(new File(getLogPath(), fileName).getAbsolutePath() + ".%d{yyyy-MM-dd-HH}");
        rolling.setMaxHistory(maxBackupIndex);
        rolling.setParent(appender);
        appender.setRollingPolicy(rolling);

        PatternLayoutEncoder layout = new PatternLayoutEncoder();
        layout.setPattern(pattern);
        layout.setCharset(Charset.forName(getEncoding()));
        appender.setEncoder(layout);
        // 启动
        appender.start();
        return appender;
    }

}
