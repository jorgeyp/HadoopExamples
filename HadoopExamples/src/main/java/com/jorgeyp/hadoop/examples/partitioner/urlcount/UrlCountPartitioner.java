package com.jorgeyp.hadoop.examples.partitioner.urlcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class UrlCountPartitioner extends Partitioner<Text, LongWritable> {

    @Override
    public int getPartition(Text key, LongWritable value, int numPartitions) {
        System.out.println("Partitions: " + numPartitions);
        if (numPartitions == 0)
            return 0;
        try {
            URL url = new URL(key.toString());
            return Math.abs(url.getHost().hashCode()) % numPartitions;
        } catch (MalformedURLException ignored) {
            return 0;
        }
    }
}
