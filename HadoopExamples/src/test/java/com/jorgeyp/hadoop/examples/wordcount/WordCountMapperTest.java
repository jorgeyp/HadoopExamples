package com.jorgeyp.hadoop.examples.wordcount;

import com.jorgeyp.hadoop.examples.wordcount.WordCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by jorge.yague on 22/01/16.
 */
public class WordCountMapperTest {

    @Test
    public void processesValidLine() throws IOException, InterruptedException {
        Text line = new Text("Lorem ipsum dolor sit amet.");

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), line)
                .withOutput(new Text("Lorem"), new IntWritable(1))
                .withOutput(new Text("ipsum"), new IntWritable(1))
                .withOutput(new Text("dolor"), new IntWritable(1))
                .withOutput(new Text("sit"), new IntWritable(1))
                .withOutput(new Text("amet."), new IntWritable(1))
                .runTest();
    }
}
