package com.dataflair.bd.poc.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MoviesRatingJoinReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<Text> listMovies = new ArrayList<Text>();
	private ArrayList<Text> listRating = new ArrayList<Text>();
	
	private Map<Text, Text> userMovie = new HashMap<Text,Text>();
	private Map<Text, ArrayList<Text>> userRating = new HashMap<Text,ArrayList<Text>>();
	
	private Map<Text, ArrayList<Text>> total = new HashMap<Text,ArrayList<Text>>();
	
	private TreeMap<Text,Text> highestView = new TreeMap<Text,Text>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		listMovies.clear();
		listRating.clear();

		String merge = "";
		
		for (Text text : values) {

			if (text.charAt(0) == 'M') 
			{
				
				//userMovie.put(key,new Text(text.toString().substring(1)));
				
				listMovies.add(new Text(text.toString().substring(1))); //title
				
				/*if(userMovie.get(key)!=null)
				{   
					//userMovie.get(key).add(new Text(text.toString().substring(1)));
					//put(key, new Text(text.toString().substring(1)));
				}
				else
				{
					 // listMovies.add(new Text(text.toString().substring(1)));
					  userMovie.put(key,new Text(text.toString().substring(1)));
					   //title
					//userMovie.put(key, value)
				}*/
					
				
			} 
			else if (text.charAt(0) == 'R') 
			{
				listRating.add(new Text(text.toString().substring(1))); //rating
				
				/*if(userRating.get(key)!=null)
				{   
					userRating.get(key).add(new Text(text.toString().substring(1)));
					//put(key, new Text(text.toString().substring(1)));
				}
				else
				{
					  listRating.add(new Text(text.toString().substring(1)));
					  userRating.put(key,listRating);
					   //title
					//userMovie.put(key, value)
				}*/
				
				
				
				
				
			}

		}

		executeJoinLogic(context);

	}

	private void executeJoinLogic(Context context) throws IOException, InterruptedException 
	{
		int count=0;
		if (!listMovies.isEmpty() && !listRating.isEmpty()) {
			for (Text moviesData : listMovies) {

				//System.out.println("MoviesData..."+""+moviesData +"listRating.."+ new Text(String.valueOf(listRating.size())));
				//context.write(moviesData, new Text(String.valueOf(listRating.size())));
				
				for (Iterator iterator = listRating.iterator(); iterator.hasNext();) {
					Text text = (Text) iterator.next();
					count++;
					
					if(count>20) 
					{
						highestView.put(moviesData,new Text(""+count));
					}
					
					if (highestView.size() > 10) {
						highestView.remove(highestView.firstKey());
					}
					
					
				}
				//context.write(new Text(""+count),moviesData);
				
						
				count=0;
				

			}
		}
		
		/*if (!userMovie.isEmpty() && !userRating.isEmpty()) 
		{
		 
		  for (Map.Entry<Text,Text> e : userMovie.entrySet())
		  {
		      Text movieID = e.getKey();
		      Text title = e.getValue();
		      
		      
		      ArrayList value = userRating.get(movieID);
		      
		      //total.put(key,value);
		      
		      if(value!=null && value.size()>0)
		      {
		      
		      
		      for (Iterator iterator = value.iterator(); iterator.hasNext();) 
		      {
				Text userId = (Text) iterator.next();
				
				context.write(new Text(movieID+","+title),userId);
				
			}

		      }
		  }
			
		}*/
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		for (Map.Entry<Text, Text> entry : highestView.entrySet()) {
			context.write(entry.getKey(), entry.getValue());
		}

	}

}
