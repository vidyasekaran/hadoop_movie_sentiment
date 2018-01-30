package com.dataflair.bd.poc.driver;

import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.dataflair.bd.proc.avg.MoviesDataMapper;
import com.dataflair.bd.proc.avg.RatingDataMapper;
import com.dataflair.bd.poc.newmapper.UserDataMapper;
import com.dataflair.bd.proc.avg.MoviesRatingJoinReducer;

public class Driver2 
{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{	
		args = new String[] { "movies.dat", "ratings.dat", "users.dat","tmp5", "tmp3" };
		
		Job sampleJob = Job.getInstance();
		sampleJob.setJarByClass(Driver2.class);
		//sampleJob.setMapperClass(MoviesDataMapper.class);
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
		
	}
}