package com.jorgeyp.hadoop.examples.writables;

import com.jorgeyp.hadoop.examples.wordcount.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class Aggregation2dTest {

    // ##########   Map     ####################################################/
    @Test
    public void processesValidLine() throws IOException, InterruptedException {
        Text line = new Text("1,2:3");

        new MapDriver<LongWritable, Text, PointWritable, LongWritable>()
                .withMapper(new Aggregation2dMapper())
                .withInput(new LongWritable(0), line)
                .withOutput(new PointWritable(1, 2), new LongWritable(3))
                .runTest();
    }

    // ##########   Reduce  ###################################################/
    @Test
    public void returnsPoints() throws IOException, InterruptedException {

        new ReduceDriver<PointWritable, LongWritable, PointWritable, LongWritable>()
                .withReducer(new Aggregation2dReducer())
                .withInput(new PointWritable(1, 2), Arrays.asList(new LongWritable(3), new LongWritable(7)))
                .withOutput(new PointWritable(1, 2), new LongWritable(10))
                .runTest();
    }

}
