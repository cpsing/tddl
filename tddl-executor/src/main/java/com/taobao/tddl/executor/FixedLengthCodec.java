package com.taobao.tddl.executor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.tddl.executor.common.CloneableRecord;
import com.taobao.tddl.executor.common.RecordCodec;
import com.taobao.tddl.optimizer.config.table.ColumnMeta;
import com.taobao.tddl.optimizer.core.expression.ISelectable.DATA_TYPE;

/**
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class FixedLengthCodec implements RecordCodec<byte[]> {

    List<ColumnMeta>     columns;
    Map<String, Integer> index;
    int                  sizeCache = 0;

    public FixedLengthCodec(List<ColumnMeta> columns){
        this.columns = columns;
        sizeCache = columns.size();
        this.index = new HashMap(sizeCache);
        for (ColumnMeta key : columns) {
            if (!index.containsKey(key.getName())) {
                index.put(key.getName(), index.size());
            }
        }
    }

    @Override
    public byte[] encode(CloneableRecord record) {
        int length = calculateEncodedLength(record, columns);
        byte[] dst = new byte[length];
        int offset = 0;
        for (ColumnMeta c : columns) {
            Object v = record.get(c.getName());
            DATA_TYPE t = c.getDataType();
            if (v == null && !c.getNullable()) {
                throw new RuntimeException(c + " is not nullable.");
            }
            offset += encode1(v, t, dst, offset);
        }
        return dst;
    }

    private int calculateEncodedLength(CloneableRecord record, List<ColumnMeta> columns) {
        int length = 0;

        for (ColumnMeta c : columns) {
            Object v = record.get(c.getName());
            DATA_TYPE t = c.getDataType();
            if (t == DATA_TYPE.LONG_VAL) {
                if (v == null) {
                    length += 1;
                } else {
                    length += 9;
                }
            } else if (t == DATA_TYPE.INT_VAL) {
                if (v == null) {
                    length += 1;
                } else {
                    length += 5;
                }
            } else if (t == DATA_TYPE.STRING_VAL) {

                if (v != null && !(v instanceof String)) {
                    v = v.toString();
                }
                length += KeyEncoder.calculateEncodedStringLength((String) v);
            } else if (t == DATA_TYPE.FLOAT_VAL) {
                length += 4;
            } else if (t == DATA_TYPE.BYTES_VAL) {
                length += KeyEncoder.calculateEncodedLength((byte[]) v);
            } else if (t == DATA_TYPE.DATE_VAL || t == DATA_TYPE.TIMESTAMP) {
                if (v == null) {
                    length += 1;
                } else {

                    length += 9;
                }
            } else {
                throw new RuntimeException("Column:" + c.getName() + " DATA_TYPE=" + t + "is not fixed length .");
            }
        }
        return length;
    }

    private int encode1(Object v, DATA_TYPE t, byte[] dst, int offset) {
        if (t == DATA_TYPE.LONG_VAL) {

            if (v instanceof BigDecimal) return DataEncoder.encode(new Long(((BigDecimal) v).longValue()), dst, offset);
            return DataEncoder.encode((Long) v, dst, offset);

        } else if (t == DATA_TYPE.INT_VAL) {
            if (v instanceof BigDecimal) return DataEncoder.encode(new Integer(((BigDecimal) v).intValue()),
                dst,
                offset);
            return DataEncoder.encode((Integer) v, dst, offset);
        } else if (t == DATA_TYPE.STRING_VAL) {
            if (v != null && !(v instanceof String)) {
                v = v.toString();
            }
            return KeyEncoder.encode((String) v, dst, offset);
        } else if (t == DATA_TYPE.FLOAT_VAL) {
            if (v instanceof BigDecimal) DataEncoder.encode(new Float(((BigDecimal) v).floatValue()), dst, offset);
            else DataEncoder.encode((Float) v, dst, offset);
            return 4;
        } else if (t == DATA_TYPE.DOUBLE_VAL) {
            if (v instanceof BigDecimal) DataEncoder.encode(new Double(((BigDecimal) v).doubleValue()), dst, offset);
            else DataEncoder.encode((Double) v, dst, offset);
            return 4;
        } else if (t == DATA_TYPE.BYTES_VAL) {
            return KeyEncoder.encode((byte[]) v, dst, offset);
        } else if (t == DATA_TYPE.DATE_VAL || t == DATA_TYPE.TIMESTAMP) {
            if (v != null && !(v instanceof Long)) {
                if (v instanceof Date) {
                    v = ((Date) v).getTime();
                } else {
                    v = ExecUtil.convertStringToDate(v.toString()).getTime();
                }
            }
            return DataEncoder.encode((Long) v, dst, offset);

        } else {
            throw new RuntimeException("DATA_TYPE=" + t + "is not supported yet.");
        }
    }

    @Override
    public CloneableRecord decode(byte[] bytes, boolean reuse) {
        if (bytes == null) {
            return null;
        }
        FixedLengthRecord record = new FixedLengthRecord(index, sizeCache);
        int offset = 0;
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta c = columns.get(i);
            DATA_TYPE t = c.getDataType();
            Object v = null;
            try {
                if (t == DATA_TYPE.LONG_VAL) {
                    v = DataDecoder.decodeLongObj(bytes, offset);
                    if (v == null) {
                        offset += 1;
                    } else {
                        offset += 9;
                    }
                } else if (t == DATA_TYPE.INT_VAL) {
                    v = DataDecoder.decodeIntegerObj(bytes, offset);
                    if (v == null) {
                        offset += 1;
                    } else {
                        offset += 5;
                    }
                } else if (t == DATA_TYPE.STRING_VAL) {
                    String[] val = new String[1];
                    offset += KeyDecoder.decodeString(bytes, offset, val);
                    v = val[0];
                } else if (t == DATA_TYPE.FLOAT_VAL) {
                    v = DataDecoder.decodeFloatObj(bytes, offset);
                    offset += 4;
                } else if (t == DATA_TYPE.BYTES_VAL) {
                    byte[][] val = new byte[1][];
                    offset += KeyDecoder.decode(bytes, offset, val);
                    v = val[0];
                } else if (t == DATA_TYPE.DATE_VAL || t == DATA_TYPE.TIMESTAMP) {

                    v = DataDecoder.decodeLongObj(bytes, offset);

                    if (v == null) {
                        offset += 1;
                    } else {
                        v = new Date((Long) v);
                        offset += 9;
                    }
                } else {
                    throw new RuntimeException("DATA_TYPE=" + t + "is not supported yet.");
                }
            } catch (CorruptEncodingException ex) {
                throw new RuntimeException("CorruptEncodingException " + t + " " + v, ex);
            }
            record.setValueByIndex(index.get(c.getName()), v);
        }
        return record;
    }

    @Override
    public CloneableRecord newEmptyRecord() {
        return new FixedLengthRecord(index, 0);
    }

}
