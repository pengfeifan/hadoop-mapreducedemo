package org.fly.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * 数据去重
 * @author williamfan
 *
 */
public class Dedup {

	//map将输入中的value复制到输出数据的key上，并直接输出
	public static class Map extends Mapper<Object,Text,Text,Text>{
		private static Text line=new Text();//每行数据
		//实现map函数
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			line=value;
			System.out.println("The process of the Map:"+key);
			context.write(line, new Text(""));
		}
	}
	//reduce将输入中的key复制到输出数据的key上
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		//
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			System.out.println("The process of the Reduce:"+key);                 
			context.write(key, new Text(""));
		}
	}
	//
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.118.128:9000");
		String[] ioArgs=new String[]{"/user/hadoop/input/dedup_in","/user/hadoop/output/dedup_out"};
		String[] otherArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		if(otherArgs.length!=2){
			System.err.println("Usage:Data Deduplicaiton <in> <out>");
			System.exit(2);
		}
		//
		Job job=new Job(conf,"Data Deduplication");
		job.setJarByClass(Dedup.class);
		//设置Map、Combine和Reduce处理类
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
