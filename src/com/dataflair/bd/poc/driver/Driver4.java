package com.dataflair.bd.poc.driver;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dataflair.bd.poc.mapper.SentimentDataMapper;
import com.gopivotal.mapreduce.lib.input.JsonInputFormat;

public class Driver4 
{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{	
		//args = new String[] { "movies.dat", "ratings.dat", "users.dat","tmp5", "tmp3" };
		
		args = new String[] { "/user/guru/tweets/FlumeData.1511316568729","tmp5"};
		
/*		Job sampleJob = Job.getInstance();
		sampleJob.setJarByClass(Driver4.class);
		sampleJob.setMapperClass(Map.class);
		sampleJob.setReducerClass(Reduce.class);
		
		sampleJob.setMapOutputKeyClass(Text.class);
		 
		sampleJob.setMapOutputValueClass(Text.class);
		 
		sampleJob.setOutputKeyClass(NullWritable.class);
		 
		sampleJob.setOutputValueClass(Text.class);
		 
		sampleJob.setInputFormatClass(TextInputFormat.class);
		 
		sampleJob.setOutputFormatClass(TextOutputFormat.class);
		 
		FileInputFormat.addInputPath(sampleJob, new Path(args[0]));
		 
		FileOutputFormat.setOutputPath(sampleJob, new Path(args[1]));
		 
		System.exit(sampleJob.waitForCompletion(true) ? 0 : 1);*/
		
		/*Job sampleJob = Job.getInstance();
		sampleJob.setJarByClass(Driver4.class);
		sampleJob.setMapperClass(SentimentDataMapper.class);
		sampleJob.setNumReduceTasks(0);
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		
		sampleJob.setOutputKeyClass(NullWritable.class);
		sampleJob.setOutputValueClass(Text.class);
		sampleJob.setInputFormatClass(TextInputFormat.class);
		sampleJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(sampleJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sampleJob, new Path(args[1]));
		System.exit(sampleJob.waitForCompletion(true) ? 0 : 1);*/
		
		Job sampleJob = Job.getInstance();
		sampleJob.setJarByClass(Driver4.class);
		sampleJob.setMapperClass(SentimentDataMapper.class);
		sampleJob.setNumReduceTasks(0);
		sampleJob.setOutputKeyClass(NullWritable.class);
		sampleJob.setOutputValueClass(Text.class);
		
		//sampleJob.setOutputKeyClass(NullWritable.class);
		//sampleJob.setOutputValueClass(Text.class);
		//sampleJob.setInputFormatClass(MultiLineJsonInputFormat.class);
		sampleJob.setInputFormatClass(JsonInputFormat.class);
		//MultiLineJsonInputFormat.setInputJsonMember(sampleJob, "id");
		
		sampleJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(sampleJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sampleJob, new Path(args[1]));
		System.exit(sampleJob.waitForCompletion(true) ? 0 : 1);
		
	
				
		
		/*
		sampleJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", "::");
		TextOutputFormat.setOutputPath(sampleJob, new Path(args[3]));
		sampleJob.setOutputKeyClass(Text.class);
		sampleJob.setOutputValueClass(Text.class);
		sampleJob.setReducerClass(MoviesRatingJoinReducer.class);
		
		MultipleInputs.addInputPath(sampleJob, new Path(args[0]), TextInputFormat.class, MoviesDataMapper.class);
		MultipleInputs.addInputPath(sampleJob, new Path(args[1]), TextInputFormat.class, RatingDataMapper.class);
		//MultipleInputs.addInputPath(sampleJob, new Path(args[2]), TextInputFormat.class, UserDataMapper.class);
		
		int code = sampleJob.waitForCompletion(true) ? 0 : 1;
		
		System.out.println("Job result"+code);
		
		
		
		Job job = new Job(conf, "SentimentAnalysis");
		 
		job.setJarByClass(Sentiment_Analysis .class);
		 
		job.setMapperClass(Map.class);
		 
		job.setReducerClass(Reduce.class);
		 
		job.setMapOutputKeyClass(Text.class);
		 
		job.setMapOutputValueClass(Text.class);
		 
		job.setOutputKeyClass(NullWritable.class);
		 
		job.setOutputValueClass(Text.class);
		 
		job.setInputFormatClass(TextInputFormat.class);
		 
		job.setOutputFormatClass(TextOutputFormat.class);
		 
		FileInputFormat.addInputPath(job, new Path(args[0]));
		 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		 
		return 0;*/
		
	}
}