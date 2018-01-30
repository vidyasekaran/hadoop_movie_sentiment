package com.dataflair.bd.poc.mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//ratings): UserID::MovieID::Rating::Timestamp ==> 1::1193::5::978300760
public class RatingDataMapper extends Mapper<Object, Text, Text, Text> {

	private Text movieId = new Text();
	private Text outvalue = new Text();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
	{
		String data = values.toString();
		String[] field = data.split("::", -1);
	
		if (null != field && field.length == 4 && field[0].length() > 0) 
		{

			movieId.set(field[1]);   //movieID
			outvalue.set("R" + field[0]);  //Rating 0 instead of 2
			//outvalue.set("," + field[0]);  //UserId
			
			context.write(movieId, outvalue);

		}

	}
}