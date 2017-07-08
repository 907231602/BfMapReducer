package com.bf.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bf.mapper.WordCountMapper;
import com.bf.reducer.WordCountReducer;

public class Test {
	public static void main(String[] args) {
		Configuration conf=new Configuration();
		
		try {
			//一个job对象代表一个作业
			Job job =Job.getInstance(conf);
			job.setJarByClass(Test.class);//在当前类
			//对作业的mapper进行设置
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(	IntWritable.class);
			
			//对作业的reduce进行设置
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			//设置要统计的日志
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/hello.txt"));
			//设置要统计的位置
			FileOutputFormat.setOutputPath(job, new Path("hdfs://yanjijun1:9000/wcc"));
			
			boolean bool=job.waitForCompletion(true);
			System.out.println(bool);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
