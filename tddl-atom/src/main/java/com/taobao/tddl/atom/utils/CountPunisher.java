package com.taobao.tddl.atom.utils;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 计数惩罚器 在最近一段时间窗口内计数超过限制则惩罚
 * 
 * <pre>
 * 
 * CountPunisher timeOutPunisher = new CountPunisher(new SmoothValve(x), 3000, 10); 3秒钟之内超时10次则惩罚
 * if(timeOutPunisher.punish()){
 *     // 获取连接时，判断为超时惩罚期，抛出该异常
 * }
 * 
 * 
 * timeOutPunisher.count(); // 判断为超时，计数一下
 * </pre>
 * 
 * @author linxuan
 */
public class CountPunisher {

    private static final Logger logger          = LoggerFactory.getLogger(CountPunisher.class);
    private final SmoothValve   smoothValve;
    private final long          timeWindow;                                                    // ms;
                                                                                                // 值为0表示关闭这个功能，无任何限制
    private final long          limit;
    private final long          resetTime;

    private volatile long       timeWindowBegin = 0;                                           // 0表示没有超时，没有计时
    private volatile long       punishBeginTime = Long.MAX_VALUE;
    private final AtomicInteger count           = new AtomicInteger();

    /**
     * timeWindow之内，超时（或其他意义）次数超过limit，则标识为punish, 这个punish标志resetTime过后自动复位
     * 
     * @param smoothValve
     * @param timeWindow
     * @param limit
     */
    public CountPunisher(SmoothValve smoothValve, long timeWindow, long limit, long resetTime){
        this.smoothValve = smoothValve;
        this.timeWindow = timeWindow;
        this.limit = limit;
        this.resetTime = resetTime;
    }

    public CountPunisher(SmoothValve smoothValve, long timeWindow, long limit){
        this(smoothValve, timeWindow, limit, 5 * 60 * 100); // 默认5分钟复位
    }

    public void count() {
        if (timeWindow == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - timeWindowBegin > timeWindow) {
            resetTimeWindow();
        }

        if (timeWindowBegin == 0) {
            timeWindowBegin = now;
        }

        if (count.incrementAndGet() >= limit) {
            punishBeginTime = now;
            smoothValve.setNotAvailable();
        }
    }

    private void resetTimeWindow() {
        timeWindowBegin = 0;
        count.set(0);
    }

    /**
     * @return true表示惩罚；false表示通过
     */
    public boolean punish() {
        if (punishBeginTime == Long.MAX_VALUE || timeWindow == 0) {
            // 这里不需要复位
            return false;
        }
        if (System.currentTimeMillis() - punishBeginTime > resetTime) {
            // 超过复位时间，不再惩罚，状态复位
            punishBeginTime = Long.MAX_VALUE;
            resetTimeWindow();
            smoothValve.setAvailable();
            return false;
        }
        return !smoothValve.smoothThroughOnInitial();
    }

    private static enum CreateProperties {
        timeWindow, limit, resetTime;
    }

    private static final String LINE_SEP = System.getProperty("line.separator");

    public static CountPunisher parse(SmoothValve smoothValve, String str) {
        str = str.replaceAll("@@@", LINE_SEP);
        str = str.replaceAll("###", LINE_SEP);
        str = str.replaceAll("$$$", LINE_SEP);
        str = str.replaceAll(";;;", LINE_SEP);
        try {
            Properties p = new Properties();
            p.load(new ByteArrayInputStream((str).getBytes()));
            long timeWindow = -1;
            long limit = -1;
            long resetTime = -1;
            for (Map.Entry<Object, Object> entry : p.entrySet()) {
                String key = ((String) entry.getKey()).trim();
                String value = ((String) entry.getValue()).trim();
                switch (CreateProperties.valueOf(key)) {
                    case timeWindow:
                        timeWindow = Long.parseLong(value);
                        break;
                    case limit:
                        limit = Long.parseLong(value);
                        break;
                    case resetTime:
                        resetTime = Long.parseLong(value);
                        break;
                    default:
                        break;
                }
            }
            if (timeWindow == -1 || limit == -1) {
                logger.error("CountPunisher Properties incomplete");
                return null;
            }
            if (resetTime == -1) {
                return new CountPunisher(smoothValve, timeWindow, limit);
            } else {
                return new CountPunisher(smoothValve, timeWindow, limit, resetTime);
            }
        } catch (Exception e) {
            logger.error("parse CountPunisher Properties failed", e);
            return null;
        }
    }

    public String toString() {
        return new StringBuilder("exceed ").append(limit)
            .append(" in ")
            .append(timeWindow)
            .append("ms, and will reset in ")
            .append(resetTime)
            .append("ms")
            .toString();
    }
}
