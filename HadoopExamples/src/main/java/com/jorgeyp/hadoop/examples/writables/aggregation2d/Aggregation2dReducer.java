package com.jorgeyp.hadoop.examples.writables.aggregation2d;

import com.jorgeyp.hadoop.examples.writables.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class Aggregation2dReducer extends Reducer<PointWritable, LongWritable, PointWritable, LongWritable> {

    private LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(PointWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        outValue.set(count);
        context.write(key, outValue);
    }
}
