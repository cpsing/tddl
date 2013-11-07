package com.taobao.tddl.monitor.logger.log4j;

import java.io.File;

import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.taobao.tddl.monitor.logger.DynamicLogger;
import com.taobao.tddl.monitor.logger.LoggerInit;

public class DynamicLog4jLogger extends DynamicLogger {

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

        org.apache.log4j.Logger logger = (Logger) LoggerInit.TDDL_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(tddlAppender);
        logger.setLevel(Level.WARN);

        logger = (Logger) LoggerInit.TDDL_MD5_TO_SQL_MAPPING.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(md5sqlAppender);
        logger.setLevel(Level.DEBUG);

        logger = (Logger) LoggerInit.TDDL_Nagios_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(nagiosAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Atom_Statistic_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(atomStatisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Matrix_Statistic_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(matrixStatisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Conn_Statistic_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(connStatisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Statistic_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(statisticAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.TDDL_Snapshot_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(snapshotAppender);
        logger.setLevel(Level.INFO);
    }

    public void initRule() {
        Appender dbTabAppender = buildAppender("TDDL_Vtab_Appender", "tddl-db-tab.log", "%m");
        Appender vSlotAppender = buildAppender("TDDL_Vtab_Appender", "tddl-vslot.log", "%m");
        Appender dynamicRuleAppender = buildAppender("TDDL_DynamicRule_Appender", "tddl-dynamic-rule.log", "%m");

        org.apache.log4j.Logger logger = (Logger) LoggerInit.DB_TAB_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(dbTabAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.VSLOT_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(vSlotAppender);
        logger.setLevel(Level.INFO);

        logger = (Logger) LoggerInit.DYNAMIC_RULE_LOG.getDelegate();
        logger.setAdditivity(false);
        logger.removeAllAppenders();
        logger.addAppender(dynamicRuleAppender);
        logger.setLevel(Level.INFO);
    }

    private Appender buildAppender(String name, String fileName, String pattern) {
        DailyRollingFileAppender appender = new DailyRollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setEncoding(getEncoding());
        appender.setLayout(new PatternLayout(pattern));
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
        appender.activateOptions();// 很重要，否则原有日志内容会被清空
        return appender;
    }

    private Appender buildDailyMaxRollingAppender(String name, String fileName, String pattern, int maxBackupIndex) {
        DailyMaxRollingFileAppender appender = new DailyMaxRollingFileAppender();
        appender.setName(name);
        appender.setAppend(true);
        appender.setEncoding(getEncoding());
        appender.setLayout(new PatternLayout(pattern));
        appender.setDatePattern("'.'yyyy-MM-dd-HH");
        appender.setMaxBackupIndex(maxBackupIndex);
        appender.setFile(new File(getLogPath(), fileName).getAbsolutePath());
        appender.activateOptions();// 很重要，否则原有日志内容会被清空
        return appender;
    }

}
