package org.fly.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class STjoin {

	public static int time=0;
	/*
	 * 
	 */
	public static class Map extends Mapper<Object,Text,Text,Text>{
		//
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String childname=new String();//
			String parentname=new String();//
			String relationType=new String();//
			//
			StringTokenizer itr=new StringTokenizer(value.toString());
			String[] values=new String[2];
			int i=0;
			while(itr.hasMoreTokens()){
				values[i]=itr.nextToken();
				i++;
			}
			if(values[0].compareTo("child")!=0){
				childname=values[0];
				parentname=values[1];
				//
				relationType="1";
				context.write(new Text(values[1]), new Text(relationType+"+"+childname+"+"+parentname));
				//
				relationType="2";
				context.write(new Text(values[0]), new Text(relationType+"+"+childname+"+"+parentname));
				
			}
		}
	}
	//
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		//
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			//
			if(0==time){
				context.write(new Text("grandchild"),new Text("grandparent"));
				time++;
			}
			int grandchildnum=0;
			String[] grandchild=new String[10];
			int grandparentnum=0;
			String[] grandparent=new String[10];
			
			Iterator iterator=values.iterator();
			while(iterator.hasNext()){
				String record=iterator.next().toString();
				int len=record.length();
				int i=2;
				if(0==len){
					continue;
				}
				//
				char relationType=record.charAt(0);
				//
				String childname=new String();
				String parentname=new String();
				//
				while(record.charAt(i)!='+'){
					childname+=record.charAt(i);
					i++;
				}
				i=i+1;
				//
				while(i<len){
					parentname+=record.charAt(i);
					i++;
				}
				//
				if('1'==relationType){
					grandchild[grandchildnum]=childname;
					grandchildnum++;
				}
				//
				if('2'==relationType){
					grandparent[grandparentnum]=parentname;
					grandparentnum++;
				}
			}
			//
			if(0!=grandchildnum &&0!=grandparentnum){
				for(int m=0;m<grandchildnum;m++)
					for(int n=0;n<grandparentnum;n++){
						context.write(new Text(grandchild[m]), new Text(grandparent[n]));
					}
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.118.128:9000");
		String[] ioArgs=new String[]{"/user/hadoop/input/STjoin_in","/user/hadoop/output/STjoint_out"};
		String[] pathArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		Job job=new Job(conf,"Single Table Join");
		job.setJarByClass(STjoin.class);
		//
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		//
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//
		FileInputFormat.addInputPath(job,new Path(pathArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(pathArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
