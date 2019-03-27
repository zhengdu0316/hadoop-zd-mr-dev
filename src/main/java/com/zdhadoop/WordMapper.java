package com.zdhadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author zhengdu
 * @date 2019/3/24
 */
public class WordMapper extends Mapper<Object, Text,Text, IntWritable> {

    private final  static  IntWritable one = new IntWritable(1);

    private Text word = new Text();

    @Override
    public void map(Object key , Text value , Context context) throws IOException, InterruptedException{

        StringTokenizer itr = new StringTokenizer(value.toString()) ;

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word,one);
        }
    }
}
