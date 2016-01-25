package com.jorgeyp.hadoop.examples.partitioner.urlcount;

import es.afm.hadoop.examples.partitioner.urlcount.counters.Counters;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class UrlCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            context.write(value, new LongWritable(1));
        } catch (MalformedURLException e) {
            context.getCounter(Counters.MALFORMED_URL).increment(1);
        }
    }
}
