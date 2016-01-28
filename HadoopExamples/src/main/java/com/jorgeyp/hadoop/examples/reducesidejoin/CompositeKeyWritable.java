package com.jorgeyp.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jorge.yague on 28/01/16.
 */
public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {
    private Text joinKey;
    private IntWritable datasetKey;

    public static final int DATASET_KEY_TEACHERS = 2;
    public static final int DATASET_KEY_DEPARTMENTS = 1;

    public CompositeKeyWritable() {
        this.joinKey = new Text();
        this.datasetKey = new IntWritable();
    }

    public CompositeKeyWritable(Text joinKey, IntWritable datasetKey) {
        this.joinKey = joinKey;
        this.datasetKey = datasetKey;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        datasetKey.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        datasetKey.readFields(in);
    }

    public IntWritable getDatasetKey() {
        return datasetKey;
    }

    public void setDatasetKey(IntWritable datasetKey) {
        this.datasetKey = datasetKey;
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(Text joinKey) {
        this.joinKey = joinKey;
    }

    public int compareTo(CompositeKeyWritable obj) {
        int result = joinKey.compareTo(obj.joinKey);
        if (result == 0)
            result = datasetKey.compareTo(obj.datasetKey);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof CompositeKeyWritable)
            return this.compareTo((CompositeKeyWritable) obj) == 0;
        return false;
    }

    @Override
    public int hashCode() {
        int result = joinKey != null ? joinKey.hashCode() : 0;
        result = 31 * result + (datasetKey != null ? datasetKey.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(datasetKey).append(", ")
                .append(joinKey).toString();
    }
}
