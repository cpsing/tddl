package com.taobao.tddl.optimizer.utils;

import java.util.concurrent.atomic.AtomicLong;

public class RequestIDGen {

    static AtomicLong currentID = new AtomicLong(0L);

    public static long genRequestID() {
        return currentID.addAndGet(1L);
    }

}
