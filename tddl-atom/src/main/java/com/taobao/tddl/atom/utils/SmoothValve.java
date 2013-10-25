package com.taobao.tddl.atom.utils;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * 平滑阀门。用于可用性状态切换。 解决不可用到可用时，瞬间的流量、连接等的暴增冲击问题
 * 第一批只允许执行1次，第二批允许执行2次，。。。最后一批之后不再限制(batchLimits)
 * 每一批的名额用尽，要间隔timeDelay毫秒后，才开始下一批名额的发放。 在这个间隔时间期间的请求直接拒绝。
 * 允许负数的limit，含义为1/n的概率去执行。具体见构造函数
 * 
 * <pre>
 * 使用列子：
 * SmoothValve smoothValve = new SmoothValve(0);
 * try {
 *    if (valve.isNotAvailable()) {
 *       // try-lock trying
 *    }else {
 *          if (valve.smoothThroughOnInitial()) {
 *              // do smooth trying
 *              
 *              if(sucessed){
 *                  valve.setAvailable();
 *              } else {
 *                // throw not available
 *              }
 *              
 *          }else {
 *              // throw not available
 *          }
 *    }
 * 
 * 
 * </pre>
 * 
 * @author linxuan
 */
public class SmoothValve {

    private static final Logger log         = LoggerFactory.getLogger(SmoothValve.class);

    private volatile boolean    available   = true;
    private volatile boolean    isInSmooth  = false;                                     // 是否在平滑期
    private final AtomicInteger count       = new AtomicInteger();
    private final AtomicInteger batchNo     = new AtomicInteger();
    private final int[]         batchLimits;
    private final AtomicInteger rejectCount = new AtomicInteger();
    private final long          timeDelay;                                               // ms
    private volatile long       timeBegin;

    public SmoothValve(long timeDelay){
        this.timeDelay = timeDelay;
        this.batchLimits = new int[] { 1, 2, 4, 8, 16, 32, 64 };
    }

    /**
     * @param timeDelay
     * @param batchLimits 允许负数的limit，为了解决cliet数量相当多时，每个client放一个请求进来都会冲垮服务器的场景：
     * >1，表示允许通过的次数 <br/>
     * -1和0作用相同，表示放弃当前尝试，只是多延迟了一些时间。所以一般不设 <br/>
     * -2表示这批尝试中，只随机放入所有client的1/2(让当前操作1/2的概率通过) <br/>
     * -3表示这批尝试中，只随机放入所有client的1/3(让当前操作1/3的概率通过) ... <br/>
     * 例如：batchLimits = new int[] { -4,-3,-2, 1, 2, 4, 8, 16, 32 };
     */
    public SmoothValve(long timeDelay, int[] batchLimits){
        this.timeDelay = timeDelay;
        this.batchLimits = batchLimits;
    }

    /**
     * @return 返回平滑控制后的可用不可用信息 true: 可用； false：不可用，或者可用但没通过限流
     */
    public boolean smoothThroughOnInitial() {
        if (timeDelay == 0) { // 相当于完全关闭这个功能
            return true;
        }
        while (isInSmooth && available) { // 已可用，且在平滑期
            int batch = batchNo.get();
            if (batch >= batchLimits.length) {
                isInSmooth = false;
                count.set(0);
                batchNo.set(0);
                rejectCount.set(0);
                return available; // 返回可用
            }

            int limit = batchLimits[batch];
            if (limit < -1) { // 0和-1不管，后面会直接放弃
                int randomInt = new Random().nextInt(-limit);
                if (randomInt == 0) {
                    return available; // 返回可用
                } else {
                    logReject(rejectCount, limit);
                    return false; // 返回不可用
                }
            }
            int current = count.get();
            while (current < limit) {
                if (count.compareAndSet(current, current + 1)) {
                    timeBegin = System.currentTimeMillis();
                    return available; // 返回可用
                }
                current = count.get();
            }

            // current >= limit
            if (System.currentTimeMillis() - timeBegin > timeDelay) {
                // timeDelay * (batch + 1) 这样做可能会人为造成聚集暴增
                // 超过时间，才设置下一个批次
                batchNo.compareAndSet(batch, batch + 1);
                // 继续循环
            } else {
                logReject(rejectCount, limit);
                return false; // 返回不可用
            }
        }
        return available;
    }

    private void logReject(AtomicInteger rejectCount, int limit) {
        int rc = rejectCount.incrementAndGet();
        log.warn("A request reject in available switch. limit=" + limit + ",rejectCount=" + rc);
    }

    /**
     * @return 返回实际可用不可用的信息
     */
    public boolean isAvailable() {
        return available;
    }

    public boolean isNotAvailable() {
        return !available;
    }

    /**
     * 设置为可用
     */
    public void setAvailable() {
        if (available) {
            return;
        }
        count.set(0);
        batchNo.set(0);
        this.isInSmooth = true;
        this.available = true;
    }

    /**
     * 设置为不可用
     */
    public void setNotAvailable() {
        if (available) {
            rejectCount.set(0);
            this.available = false;
        }
    }

    private static enum CreateProperties {
        timeDelay, batchLimits;
    }

    public static SmoothValve parse(String str) {
        try {
            Properties p = new Properties();
            p.load(new ByteArrayInputStream((str).getBytes()));
            long td = 0;
            String[] limits = null;
            for (Map.Entry<Object, Object> entry : p.entrySet()) {
                String key = ((String) entry.getKey()).trim();
                String value = ((String) entry.getValue()).trim();
                switch (CreateProperties.valueOf(key)) {
                    case timeDelay:
                        td = Integer.parseInt(value);
                        break;
                    case batchLimits:
                        limits = value.split("\\|");
                        break;
                    default:
                        break;
                }
            }
            if (td == 0) {
                log.error("SmoothValve Properties incomplete");
                return null;
            }
            if (limits != null) {
                int[] limitArray = new int[limits.length];
                for (int i = 0; i < limits.length; i++) {
                    limitArray[i] = Integer.parseInt(limits[i].trim());
                }
                return new SmoothValve(td, limitArray);
            } else {
                return new SmoothValve(td);
            }

        } catch (Exception e) {
            log.error("parse SmoothValve Properties failed", e);
            return null;
        }
    }

    public String toString() {
        return new StringBuilder("timeDelay=").append(timeDelay)
            .append(",batchLimits=")
            .append(Arrays.toString(batchLimits))
            .toString();
    }

}
