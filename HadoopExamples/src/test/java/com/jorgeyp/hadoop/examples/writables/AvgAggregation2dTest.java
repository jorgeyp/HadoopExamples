package com.jorgeyp.hadoop.examples.writables;

import com.jorgeyp.hadoop.examples.writables.avgaggregation2d.AvgAggregation2dMapper;
import com.jorgeyp.hadoop.examples.writables.avgaggregation2d.AvgAggregation2dReducer;
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
public class AvgAggregation2dTest {

    // ##########   Map     ####################################################/
    @Test
    public void processesValidLine() throws IOException, InterruptedException {
        Text line = new Text("1,2:3");

        new MapDriver<LongWritable, Text, PointWritable, AvgWritable>()
                .withMapper(new AvgAggregation2dMapper())
                .withInput(new LongWritable(0), line)
                .withOutput(new PointWritable(1, 2), new AvgWritable(1, 3))
                .runTest();
    }

    // ##########   Reduce  ###################################################/
    @Test
    public void returnsPoints() throws IOException, InterruptedException {

        new ReduceDriver<PointWritable, AvgWritable, PointWritable, AvgWritable>()
                .withReducer(new AvgAggregation2dReducer())
                .withInput(new PointWritable(1, 2), Arrays.asList(new AvgWritable(1, 3), new AvgWritable(1, 7)))
                .withOutput(new PointWritable(1, 2), new AvgWritable(2, 10))
                .runTest();
    }

}
