package com.dataflair.bd.poc.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.dataflair.bd.poc.vo.UserActivityVO;

public class MoviesRatingJoinReducerVO extends Reducer<Text, List<UserActivityVO>, Integer, UserActivityVO> {

	private ArrayList<UserActivityVO> listMovies = new ArrayList<UserActivityVO>();
	private ArrayList<UserActivityVO> listRating = new ArrayList<UserActivityVO>();
	
	private TreeMap<Integer, UserActivityVO> highestView = new TreeMap<Integer, UserActivityVO>();

	//@Override
	public void reduce(Text key, Iterable<UserActivityVO> values, Context context) throws IOException, InterruptedException {

		listMovies.clear();
		listRating.clear();
		int count =1;

		String data ="";
		
		
		for ( UserActivityVO userActivity : values) 
		{
			count += userActivity.getCount();
			//context.write(key, userActivity);
			
			/*data = userActivity.toString();
			String[] field = data.split(",", -1);
			String[] f = ((field[4].trim()).split("=",-1));
			
			rate += Integer.parseInt(f[1]);
			rate += userActivity.getRating();*/
				
			if (count>20 && highestView.size() > 10) {
				highestView.remove(highestView.firstKey());	
			}	
			//context.write(count, userActivity);
			highestView.put(count, userActivity);
		}
		
		
		//highestView.put(rate, userActivity1);
		
		/*for (Text t : highestView.descendingMap().values()) 
		{

			context.write(highestView., t);

		}*/
		
		

	executeJoinLogic(context);

	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException 
	{
		
		for (Integer t : highestView.descendingMap().keySet()) 
		{
			UserActivityVO tk = highestView.get(t);
			context.write(t, tk);

		}

		/*if (!listMovies.isEmpty() && !listRating.isEmpty()) {
			for (Text moviesData : listMovies) {

				context.write(moviesData, new Text(String.valueOf(listRating.size())));

			}
		}*/	
		
	}
}
