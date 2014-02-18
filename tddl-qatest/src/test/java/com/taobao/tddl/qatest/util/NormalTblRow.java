package com.taobao.tddl.qatest.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Comment for NormalTblRow
 * <p/>
 * Author By: zhuoxue.yll Created Date: 2013-4-8 上午10:41:49
 */
public class NormalTblRow {

    private Long       pk;
    private String     varcharr;
    private String     charr;
    private String     varbinaryr;
    private String     binaryr;
    private byte[]     blobr;
    private String     textr;
    private Integer    tinyintr;
    private Integer    smallintr;
    private Integer    mediumintr;
    private Integer    integerr;
    private Long       bigintr;
    private Integer    utinyintr;
    private Integer    usmallintr;
    private Integer    umediumintr;
    private Long       uintegerr;
    private BigDecimal ubigintr;
    private Boolean    bitr;
    private Float      floatr;
    private Double     doubler;
    private BigDecimal decimalr;

    private Date       dater;
    private String     dateString;
    private Time       timer;
    private Timestamp  datetimer;
    private Timestamp  timestampr;
    private Date       yearr;
    private Date       datetimeDate;

    public long getPk() {
        return pk;
    }

    public void setPk(long pk) {
        this.pk = pk;
    }

    public String getVarcharr() {
        return varcharr;
    }

    public void setVarcharr(String varcharr) {
        this.varcharr = varcharr;
    }

    public String getCharr() {
        return charr;
    }

    public void setCharr(String charr) {
        this.charr = charr;
    }

    public String getVarbinaryr() {
        return varbinaryr;
    }

    public void setVarbinaryr(String varbinaryr) {
        this.varbinaryr = varbinaryr;
    }

    public String getBinaryr() {
        return binaryr;
    }

    public void setBinaryr(String binaryr) {
        this.binaryr = binaryr;
    }

    public byte[] getBlobr() {
        return blobr;
    }

    public void setBlobr(byte[] blobr) {
        this.blobr = blobr;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public String getTextr() {
        return textr;
    }

    public void setTextr(String textr) {
        this.textr = textr;
    }

    public Integer getTinyintr() {
        return tinyintr;
    }

    public void setTinyintr(Integer tinyintr) {
        this.tinyintr = tinyintr;
    }

    public Integer getSmallintr() {
        return smallintr;
    }

    public void setSmallintr(Integer smallintr) {
        this.smallintr = smallintr;
    }

    public Integer getMediumintr() {
        return mediumintr;
    }

    public void setMediumintr(Integer mediumintr) {
        this.mediumintr = mediumintr;
    }

    public Integer getIntegerr() {
        return integerr;
    }

    public void setIntegerr(Integer integerr) {
        this.integerr = integerr;
    }

    public Long getBigintr() {
        return bigintr;
    }

    public void setBigintr(Long bigintr) {
        this.bigintr = bigintr;
    }

    public Integer getUtinyintr() {
        return utinyintr;
    }

    public void setUtinyintr(Integer utinyintr) {
        this.utinyintr = utinyintr;
    }

    public Integer getUsmallintr() {
        return usmallintr;
    }

    public void setUsmallintr(Integer usmallintr) {
        this.usmallintr = usmallintr;
    }

    public Integer getUmediumintr() {
        return umediumintr;
    }

    public void setUmediumintr(Integer umediumintr) {
        this.umediumintr = umediumintr;
    }

    public Long getUintegerr() {
        return uintegerr;
    }

    public void setUintegerr(Long uintegerr) {
        this.uintegerr = uintegerr;
    }

    public BigDecimal getUbigintr() {
        return ubigintr;
    }

    public void setUbigintr(BigDecimal ubigintr) {
        this.ubigintr = ubigintr;
    }

    public Boolean getBitr() {
        return bitr;
    }

    public void setBitr(Boolean bitr) {
        this.bitr = bitr;
    }

    public Float getFloatr() {
        return floatr;
    }

    public void setFloatr(Float floatr) {
        this.floatr = floatr;
    }

    public Double getDoubler() {
        return doubler;
    }

    public void setDoubler(Double doubler) {
        this.doubler = doubler;
    }

    public BigDecimal getDecimalr() {
        return decimalr;
    }

    public void setDecimalr(BigDecimal decimalr) {
        this.decimalr = decimalr;
    }

    public Date getDater() {
        return dater;
    }

    public void setDater(Date dater) {
        this.dater = dater;
    }

    public String getDateString() {
        return dateString;
    }

    public void setDateString(String dateString) {
        this.dateString = dateString;
    }

    public Time getTimer() {
        return timer;
    }

    public void setTimer(Time timer) {
        this.timer = timer;
    }

    public Timestamp getDatetimer() {
        return datetimer;
    }

    public void setDatetimer(Timestamp datetimer) {
        this.datetimer = datetimer;
    }

    public Timestamp getTimestampr() {
        return timestampr;
    }

    public void setTimestampr(Timestamp timestampr) {
        this.timestampr = timestampr;
    }

    public Date getYearr() {
        return yearr;
    }

    public void setYearr(Date yearr) {
        this.yearr = yearr;
    }

    public Date getDatetimeDate() {
        return datetimeDate;
    }

    public void setDatetimeDate(Date datetimeDate) {
        this.datetimeDate = datetimeDate;
    }

}
