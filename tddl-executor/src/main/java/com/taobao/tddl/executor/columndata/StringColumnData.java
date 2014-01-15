package com.taobao.tddl.executor.columndata;

/**
 * @author mengshi.sunmengshi 2014年1月14日 上午11:02:43
 * @since 5.1.0
 */
public class StringColumnData implements ColumnData {

    protected final String value;

    public StringColumnData(String value){
        this.value = value;
    }

    @Override
    public int compareTo(ColumnData o) {
        if (o.isNull()) {
            return 1;
        }
        // return this.value.compareTo((String) o.getValue());
        return this.compare(value, (String) o.getValue());
        // return this.value.compareToIgnoreCase((String) o.getValue());
    }

    public int compare(String s1, String s2) {
        int n1 = s1.length(), n2 = s2.length();
        for (int i1 = 0, i2 = 0; i1 < n1 && i2 < n2; i1++, i2++) {
            char c1 = s1.charAt(i1);
            char c2 = s2.charAt(i2);
            if (c1 != c2) {
                c1 = Character.toLowerCase(c1);
                c2 = Character.toLowerCase(c2);
                if (c1 != c2) {
                    c1 = Character.toUpperCase(c1);
                    c2 = Character.toUpperCase(c2);
                    if (c1 != c2) {
                        return c1 - c2;
                    }
                }
            }
        }
        return n1 - n2;
    }

    @Override
    public ColumnData add(ColumnData cv) {
        throw new UnsupportedOperationException("StringColumnData not supported add(ColumnData cv)!");
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public boolean equals(ColumnData cv) {
        if (cv.isNull() && this.value != null) {
            return false;
        } else if (!cv.isNull() && this.value == null) {
            return false;
        } else if (cv.isNull() && this.value == null) {
            return true;
        }

        if (cv.getValue().equals(this.value)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean isNull() {
        return this.value == null;
    }

    @Override
    public String toString() {
        return this.value;
    }
}
