package com.jorgeyp.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by jorge.yague on 28/01/16.
 */
public class CompositeKeyWritableGroupingComparator extends WritableComparator {
    protected CompositeKeyWritableGroupingComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyWritable w1 = (CompositeKeyWritable) a;
        CompositeKeyWritable w2 = (CompositeKeyWritable) b;
        return w1.getJoinKey().compareTo(w2.getJoinKey());
    }
}
