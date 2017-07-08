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
			//һ��job�������һ����ҵ
			Job job =Job.getInstance(conf);
			job.setJarByClass(Test.class);//�ڵ�ǰ��
			//����ҵ��mapper��������
			job.setMapperClass(WordCountMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(	IntWritable.class);
			
			//����ҵ��reduce��������
			job.setReducerClass(WordCountReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			//����Ҫͳ�Ƶ���־
			FileInputFormat.setInputPaths(job, new Path("hdfs://yanjijun1:9000/hello.txt"));
			//����Ҫͳ�Ƶ�λ��
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
