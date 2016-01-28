package com.jorgeyp.hadoop.examples.reducesidejoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by jorge.yague on 28/01/16.
 */
public class ReduceSideJoinPartitioner extends Partitioner<CompositeKeyWritable, Text> {

    @Override
    public int getPartition(CompositeKeyWritable key, Text value, int numPartitions) {
        return Math.abs(key.getJoinKey().hashCode()) % numPartitions;
    }
}
