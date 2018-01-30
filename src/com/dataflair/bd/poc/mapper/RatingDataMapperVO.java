package com.dataflair.bd.poc.mapper;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dataflair.bd.poc.vo.UserActivityVO;

//ratings): UserID::MovieID::Rating::Timestamp ==> 1::1193::5::978300760
public class RatingDataMapperVO extends Mapper<Object, Text, Text, UserActivityVO> 
{
	
	

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException 
	{
		UserActivityVO user = new UserActivityVO();
		String data = values.toString();
		String[] field = data.split("::", -1);
		if (null != field && field.length == 4 && field[0].length() > 0) {

			/*movieId.set(field[1]);   //movieID
			outvalue.set("R" + field[2]);  //Rating
			context.write(movieId, outvalue);*/
			
			user.setUserId(Integer.parseInt(field[0]));
			user.setMovieId(field[1]);
			user.setRating(Integer.parseInt(field[2]));
			
			//context.write(new Text(user.getMovieId()), new Text(String.valueOf(user)));
			context.write(new Text(field[1]), user);

		}

	}
}