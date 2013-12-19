package com.taobao.tddl.repo.bdb.spi;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.utilint.LoggerUtils;

public class CleanerManage {

    /**
     * add by shenxun : god hook to stop all cleaner Invoked by Env.
     * 全局变量，用于外部控制cleaner的行为
     */
    public static volatile boolean stopCurrentCleaners = false;

    public static volatile boolean isWriteLog          = true;

    public static void writeLog(Logger logger, EnvironmentImpl env, String logMsg) {
        if (isWriteLog) {
            LoggerUtils.logMsg(logger, env, Level.WARNING, logMsg);
        }

    }

    public static boolean isStopCurrentCleaners() {
        return stopCurrentCleaners;
    }

    public static void setStopCurrentCleaners(boolean stopCurrentCleaners) {
        CleanerManage.stopCurrentCleaners = stopCurrentCleaners;
        System.err.println("now cleaner is been stoped");
    }

    public static void stopCurrentCleaners() {
        setStopCurrentCleaners(true);
    }

    public static void startCurrentCleaners() {
        setStopCurrentCleaners(false);
    }

    public static boolean isWriteLog() {
        return isWriteLog;
    }

    public static void stopWriteLog() {
        setWriteLog(false);
    }

    public static void startWriteLog() {
        setWriteLog(true);
    }

    public static void setWriteLog(boolean isWriteLog) {
        CleanerManage.isWriteLog = isWriteLog;
    }

}
