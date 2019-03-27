package com.zdhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "master:9001");
        try {
            Job job = new Job(conf);
            //提交代码
            job.setJarByClass(App.class);
            job.setMapperClass(WordMapper.class);
            job.setReducerClass(WordReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
//			job.setNumReduceTasks(1)//设置reducer任务的个数，默认为1
            //mapreducer 输入数据所在文件或目录
            FileInputFormat.addInputPath(job, new Path("/usr/input/wc"));
            //mr执行之后的输出目录
            FileOutputFormat.setOutputPath(job, new Path("/usr/output/wc"));
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
