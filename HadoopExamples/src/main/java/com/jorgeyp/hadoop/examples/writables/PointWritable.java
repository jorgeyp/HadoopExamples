package com.jorgeyp.hadoop.examples.writables;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class PointWritable implements WritableComparable<PointWritable> {
    private DoubleWritable x;
    private DoubleWritable y;

    // Required by MapReduce
    public PointWritable() {
        this.x = new DoubleWritable();
        this.y = new DoubleWritable();
    }

    public PointWritable(double x, double y) {
        this.x = new DoubleWritable(x);
        this.y = new DoubleWritable(y);
    }

    @Override
    public int compareTo(PointWritable o) {

        if (x.compareTo(o.getX()) == 0) {
            return y.compareTo(o.getY());
        }

        return x.compareTo(o.getX());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        x.write(dataOutput);
        y.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        x.readFields(dataInput);
        y.readFields(dataInput);
    }

    public DoubleWritable getX() {
        return x;
    }

    public void setX(DoubleWritable x) {
        this.x = x;
    }

    public DoubleWritable getY() {
        return y;
    }

    public void setY(DoubleWritable y) {
        this.y = y;
    }

    /*MRUnit needs hashcode and equals in order to compare outputs*/
    @Override
    public int hashCode() {
        return x.hashCode()*128 + y.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof PointWritable){
            if(this.compareTo((PointWritable) obj) == 0)
                return true;
        }
        return false;
    }

    // Write to the output file
    @Override
    public String toString() {
        return "[" + x.get() + ", " + y.get() + ']';
    }
}
