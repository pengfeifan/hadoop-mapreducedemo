package org.fly.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 平均成绩
 * @author hadoop
 *
 */
public class Score {

	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		//
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			//
			String line=value.toString();
			//
			StringTokenizer tokenizerArticle=new StringTokenizer(line,"\n");
			//
			while(tokenizerArticle.hasMoreElements()){
				//
				StringTokenizer tokenizerLine=new StringTokenizer(tokenizerArticle.nextToken());
				String strName=tokenizerLine.nextToken();//
				String strScore=tokenizerLine.nextToken();//
				Text name=new Text(strName);
				int scoreInt=Integer.parseInt(strScore);
				//
				context.write(name,new IntWritable(scoreInt));
			}
		}
	}
	//
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		//
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			int count=0;
			Iterator<IntWritable> iterator=values.iterator();
			while(iterator.hasNext()){
				sum+=iterator.next().get();//
				count++;//
			}
			int average=(int)sum/count;
			context.write(key, new IntWritable(average));
		}
	}
	//
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://192.168.118.128:9000");
		String[] ioArgs=new String[]{"/user/hadoop/input/score_in","/user/hadoop/output/score_out"};
		String[] pathArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		//
		Job job=new Job(conf,"Score Average");
		job.setJarByClass(Score.class);
		//
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//
		job.setInputFormatClass(TextInputFormat.class);
		//
		job.setOutputFormatClass(TextOutputFormat.class);
		//
		FileInputFormat.addInputPath(job,new Path(pathArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(pathArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
