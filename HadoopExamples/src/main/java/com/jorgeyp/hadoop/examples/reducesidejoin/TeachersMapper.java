package com.jorgeyp.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jorge.yague on 28/01/16.
 */
public class TeachersMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text> {
    private static final String SEPARATOR = ",";

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fields[] = value.toString().split(SEPARATOR);

        CompositeKeyWritable outKey = new CompositeKeyWritable(new Text(fields[1]), new IntWritable(CompositeKeyWritable.DATASET_KEY_TEACHERS));

        context.write(outKey, value);
    }
}
