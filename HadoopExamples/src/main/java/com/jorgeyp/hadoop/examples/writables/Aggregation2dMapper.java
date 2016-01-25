package com.jorgeyp.hadoop.examples.writables;

import com.jorgeyp.hadoop.examples.writables.counters.Counters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class Aggregation2dMapper extends Mapper<LongWritable, Text, PointWritable, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String[] values = value.toString().split(":");
            String coordsString = values[0];
            String coordsValue = values[1];
            String[] coords = coordsString.split(",");

            LongWritable outValue = new LongWritable(Long.parseLong(coordsValue));
            PointWritable outKey = new PointWritable(Double.parseDouble(coords[0]), Double.parseDouble(coords[1]));

            context.write(outKey, outValue);
        } catch (NumberFormatException e) {
            context.getCounter(Counters.WRONG_VALUE).increment(1);
        } catch (ArrayIndexOutOfBoundsException e) {
            context.getCounter(Counters.MALFORMED_RECORD).increment(1);
        }
    }
}
