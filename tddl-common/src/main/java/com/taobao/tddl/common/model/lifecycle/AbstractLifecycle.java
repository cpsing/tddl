package com.taobao.tddl.common.model.lifecycle;

/**
 * @author jianghang 2013-10-23 下午5:19:38
 * @since 5.1.0
 */
public class AbstractLifecycle implements Lifecycle {

    private final Object              lock  = new Object();
    protected volatile LifecycleState state = LifecycleState.STOPPED;

    public void start() {
        synchronized (lock) {
            if (isRunning()) {
                return;
            }
            state = LifecycleState.RUNNING;
            init();
        }
    }

    public void stop(String why) {
        synchronized (lock) {
            if (!isRunning()) {
                return;
            }
            doStop(why);
            state = LifecycleState.STOPPED;
        }
    }

    public void stop() {
        stop("caller doesn't tell any reason to stop");
    }

    public void abort(String why, Throwable e) {
        synchronized (lock) {
            if (!isRunning()) {
                return;
            }
            doAbort(why, e);
            state = LifecycleState.ABORTED;
        }
    }

    public boolean isAborted() {
        return state.isAborted();
    }

    public boolean isRunning() {
        return state.isRunning();
    }

    public boolean isStopped() {
        return state.isStopped() || state.isAborted();
    }

    /**
     * 保留为init名字，兼容老的代码习惯
     */
    protected void init() {
        doStart();
    }

    protected void doStart() {
    }

    protected void doStop(String why) {
    }

    protected void doAbort(String why, Throwable e) {
    }

}
