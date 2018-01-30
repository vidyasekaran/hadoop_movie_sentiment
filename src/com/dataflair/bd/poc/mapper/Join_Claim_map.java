package com.dataflair.bd.poc.mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Join_Claim_map extends Mapper<Text, TupleWritable, Text, Text> 
{
	String o_value = "";

	public void map(Text key, TupleWritable value, Context cont) throws IOException, InterruptedException {
		
		for (Writable writable : value) 
		{
			o_value = o_value.concat(writable.toString()).concat(" , ");
		}
		o_value = o_value.substring(0, (o_value.length() - 3));
		cont.write(key, new Text(o_value));
		o_value = "";
	}

}