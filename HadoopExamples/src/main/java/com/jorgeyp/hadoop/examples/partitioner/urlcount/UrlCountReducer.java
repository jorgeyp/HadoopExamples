package com.jorgeyp.hadoop.examples.partitioner.urlcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class UrlCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;

        for (LongWritable value : values) {
            count += value.get();
        }

        outValue.set(count);
        context.write(key, outValue);
    }
}
