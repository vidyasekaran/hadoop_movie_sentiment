package com.dataflair.bd.poc.driver;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.alexholmes.json.mapreduce.MultiLineJsonInputFormat;
import com.dataflair.bd.poc.mapper.SentimentDataMapper;

public class Driver5 
{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{	
		//args = new String[] { "movies.dat", "ratings.dat", "users.dat","tmp5", "tmp3" };
		
		args = new String[] { "/user/guru/tweets/FlumeData.1511316568729","tmp5"};
		
		Job sampleJob = Job.getInstance();
		sampleJob.setJarByClass(Driver5.class);
		sampleJob.setMapperClass(SentimentDataMapper.class);
		
		
		//sampleJob.setNumReduceTasks(0);
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		sampleJob.setInputFormatClass(MultiLineJsonInputFormat.class);
		sampleJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(sampleJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sampleJob, new Path(args[1]));
		MultiLineJsonInputFormat.setInputJsonMember(sampleJob, "text");
		System.exit(sampleJob.waitForCompletion(true) ? 0 : 1);
		
	}
}