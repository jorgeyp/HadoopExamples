package com.jorgeyp.hadoop.examples.writables.avgaggregation2d;

import com.jorgeyp.hadoop.examples.writables.AvgWritable;
import com.jorgeyp.hadoop.examples.writables.PointWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class AvgAggregation2dReducer extends Reducer<PointWritable, AvgWritable, PointWritable, AvgWritable> {

    @Override
    protected void reduce(PointWritable key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        long sum = 0;
        for (AvgWritable value : values) {
            count += value.getCount().get();
            sum += value.getSum().get();
        }
        context.write(key, new AvgWritable(count, sum));
    }
}
