package com.jorgeyp.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jorge.yague on 28/01/16.
 */
public class ReduceSideJoinReducer extends Reducer<CompositeKeyWritable, Text, NullWritable, Text> {
    private static final String SEPARATOR = ",";

    @Override
    protected void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] fields;
        boolean first = true;
        String dptName = null;

        StringBuilder sb = new StringBuilder();
        for (Text value: values) {
            fields = value.toString().split(SEPARATOR);
            if (first) {
                // department
                first = false;
                dptName = fields[1];
            } else {
                // teacher
                sb.append(dptName).append(SEPARATOR)
                    .append(fields[2]).append(SEPARATOR) // name
                    .append(fields[3]); // salary

                context.write(NullWritable.get(), new Text(sb.toString()));
                sb.setLength(0);
            }
        }

    }
}
