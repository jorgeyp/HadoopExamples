package com.jorgeyp.hadoop.examples.partitioner;

import com.jorgeyp.hadoop.examples.wordcount.WordCountReducer;
import es.afm.hadoop.examples.partitioner.urlcount.URLCountMapper;
import es.afm.hadoop.examples.partitioner.urlcount.URLCountReducer;
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
public class URLCountTest {

    @Test
    public void processesValidLine() throws IOException, InterruptedException {
        Text line = new Text("http://www.jorgeyp.com/projects");

        new MapDriver<LongWritable, Text, Text, LongWritable>()
                .withMapper(new URLCountMapper())
                .withInput(new LongWritable(0), line)
                .withOutput(new Text("http://www.jorgeyp.com/projects"), new LongWritable(1))
                .runTest();
    }

    @Test
    public void returnsUrlCount() throws IOException, InterruptedException {
        new ReduceDriver<Text, LongWritable, Text, LongWritable>()
                .withReducer(new URLCountReducer())
                .withInput(new Text("http://www.jorgeyp.com/projects"),
                        Arrays.asList(new LongWritable(1), new LongWritable(1)))
                .withOutput(new Text("http://www.jorgeyp.com/projects"), new LongWritable(2))
                .runTest();
    }

}
