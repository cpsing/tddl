package com.taobao.tddl.repo.mysql.sqlconvertor;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * 一组sql
 * 
 * @author whisper
 */
public class Sqls {

    List<Sql> sqls = new LinkedList<Sql>();

    public void add(int arg0, Sql arg1) {
        sqls.add(arg0, arg1);
    }

    public boolean add(Sql arg0) {
        return sqls.add(arg0);
    }

    public boolean addAll(Collection<? extends Sql> arg0) {
        return sqls.addAll(arg0);
    }

    public boolean addAll(int arg0, Collection<? extends Sql> arg1) {
        return sqls.addAll(arg0, arg1);
    }

    public void clear() {
        sqls.clear();
    }

    public boolean contains(Object arg0) {
        return sqls.contains(arg0);
    }

    public boolean containsAll(Collection<?> arg0) {
        return sqls.containsAll(arg0);
    }

    public boolean equals(Object arg0) {
        return sqls.equals(arg0);
    }

    public Sql get(int arg0) {
        return sqls.get(arg0);
    }

    public int hashCode() {
        return sqls.hashCode();
    }

    public int indexOf(Object arg0) {
        return sqls.indexOf(arg0);
    }

    public boolean isEmpty() {
        return sqls.isEmpty();
    }

    public Iterator<Sql> iterator() {
        return sqls.iterator();
    }

    public int lastIndexOf(Object arg0) {
        return sqls.lastIndexOf(arg0);
    }

    public ListIterator<Sql> listIterator() {
        return sqls.listIterator();
    }

    public ListIterator<Sql> listIterator(int arg0) {
        return sqls.listIterator(arg0);
    }

    public Sql remove(int arg0) {
        return sqls.remove(arg0);
    }

    public boolean remove(Object arg0) {
        return sqls.remove(arg0);
    }

    public boolean removeAll(Collection<?> arg0) {
        return sqls.removeAll(arg0);
    }

    public boolean retainAll(Collection<?> arg0) {
        return sqls.retainAll(arg0);
    }

    public Sql set(int arg0, Sql arg1) {
        return sqls.set(arg0, arg1);
    }

    public int size() {
        return sqls.size();
    }

    public List<Sql> subList(int arg0, int arg1) {
        return sqls.subList(arg0, arg1);
    }

    public Object[] toArray() {
        return sqls.toArray();
    }

    public <T> T[] toArray(T[] arg0) {
        return sqls.toArray(arg0);
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder = new StringBuilder();
        if (sqls != null) {
            builder.append("sqls:\n\t");
            builder.append(sqls.subList(0, Math.min(sqls.size(), maxLen)));
        }
        return builder.toString();
    }

}
