package com.dataflair.bd.poc.newmapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//MovieID::Title::Genres ==> 1::Toy Story (1995)::Animation|Children's|Comedy (ratings): 
public class MoviesDataMapper extends Mapper<Object, Text, Text, Text> 
{

	private Text movieId = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException
	{
		String data = values.toString();
		String[] field = data.split("::", -1);
		
		if (null != field && field.length == 3 && field[0].length() > 0) 
		{
			movieId.set(field[0]); //movieID
			outvalue.set("M" + field[1]); //Title
			//outvalue.set(","+field[2]);   //Genres
			
			context.write(movieId, outvalue);
		}

	}
}