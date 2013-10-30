package com.taobao.tddl.rule.utils.sample;

public class SamplesCtx {

    public final static int merge   = 0;
    public final static int replace = 1;
    public final Samples    samples;
    public final int        dealType;

    public SamplesCtx(Samples commonSamples, int dealType){
        this.samples = commonSamples;
        this.dealType = dealType;
    }
}
