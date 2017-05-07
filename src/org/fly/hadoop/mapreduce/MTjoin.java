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

public class MTjoin {

	public static int time=0;
	/*
	 * 
	 */
	public static class Map extends Mapper<Object,Text,Text,Text>{
		//
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString();//
			String relationType=new String();//
			//
			if(line.contains("factoryname")==true || line.contains("addressed")==true){
				return;
			}
			//
			StringTokenizer itr=new StringTokenizer(line);
			String mapkey=new String();
			String mapvalue=new String();
			int i=0;
			while(itr.hasMoreTokens()){
				//
				String token=itr.nextToken();
				
				if(token.charAt(0)>='0' && token.charAt(0)<='9'){
					mapkey=token;
					if(i>0){
						relationType="1";
					}else{
						relationType="2";
					}
					continue;
				}
				//
				mapvalue+=token+" ";
				i++;
			}
			//
			context.write(new Text(mapkey), new Text(relationType+"+"+mapvalue));
		}
	}
	/*
	 * 
	 */
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		//
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			//
			if(0==time){
				context.write(new Text("factoryname"), new Text("addressname"));
				time++;
			}
			int factorynum=0;
			String[] factory=new String[10];
			int addressnum=0;
			String[] address=new String[10];
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
				if('1'==relationType){
					factory[factorynum]=record.substring(i);
					factorynum++;
				}
				//
				if('2'==relationType){
					address[addressnum]=record.substring(i);
					addressnum++;
				}
			}
			//
			if(0!=factorynum && 0!=addressnum){
				for(int m=0;m<factorynum;m++){
					for(int n=0;n<addressnum;n++){
						//
						context.write(new Text(factory[m]), new Text(address[n]));
					}
				}
			}
		}
	}
	//
	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		conf.set("fs.default.name","hdfs://192.168.118.128:9000");
		String[] ioArgs=new String[]{"/user/hadoop/input/MTjoin_in","/user/hadoop/output/MTjoin_out"};
		String[] pathArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		Job job=new Job(conf,"Multiple Table Join");
		job.setJarByClass(MTjoin.class);
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