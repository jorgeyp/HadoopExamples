package com.jorgeyp.hadoop.examples.partitioner.totalorderv1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class TotalOrderV1Partitioner extends Partitioner<LongWritable, Text> {

    @Override
    public int getPartition(LongWritable key, Text value, int numPartitions) {
        if (numPartitions == 0)
            return 0; // Partition 0
        // Hardcoded. Pretty ugly
        if (key.get() >= 3)
            return 1; // Partition 1
        return 0;
    }
}
