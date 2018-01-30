package com.dataflair.bd.poc.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.dataflair.bd.poc.vo.UserActivityVO;


//MovieID::Title::Genres ==> 1::Toy Story (1995)::Animation|Children's|Comedy (ratings): 
public class MoviesDataMapperVO extends Mapper<Object, Text, Text, List<UserActivityVO>>
{
	private UserActivityVO user = null;
	
	private Map<String, List<UserActivityVO>> userMap = new HashMap<String, List<UserActivityVO>>();
	
	 private Map<Integer, List<String>> userMap1 = new HashMap<Integer, List<String>>();

	@Override
	public void map(Object key, Text values, Context context) throws IOException, InterruptedException
	{
		String data = values.toString();
		String[] field = data.split("::", -1);
		
		user = new UserActivityVO();
		
		if (null != field && field.length == 3 && field[0].length() > 0 && field[1].length() > 0) 
		{
			/*movieId.set(field[0]); //movieID
			outvalue.set("M" + field[1]); //Title
			context.write(movieId, outvalue);*/
			
			user.setMovieId(field[0]);
			user.setTitle(field[1]);
			//user.setGenre(field[2]);
			
			ArrayList<UserActivityVO> user1ist1 = (ArrayList<UserActivityVO>) userMap.get(field[0]);
			
			for (Iterator iterator = user1ist1.iterator(); iterator.hasNext();) 
			{
				UserActivityVO user1 = (UserActivityVO) iterator.next();
				
		        user1.setMovieId(user.getMovieId());
		        user1.setTitle(user.getTitle());
		       // user1.setGenre(user.getGenre());
				user1.setUserId(user1.getUserId());
				//user1.setRating(user1.getRating());
				user1.setCount(1);
			}

			
			//user.setFlag("M");
			//context.write(new Text(user.getMovieId()), new Text(String.valueOf(user)));
			context.write(new Text(user.getMovieId()), user1ist1);
		}

	}
	
	//@Override
	protected void setup(Mapper<Object, Text, Text, List<UserActivityVO>>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		 loadUserInMemory(context);
	}	 
	 
	     //Loading rating file - UserID::MovieID::Rating::Timestamp ==> 1::1193::5::978300760
	 	 private void loadUserInMemory(Mapper<Object, Text, Text, List<UserActivityVO>>.Context context) {
	 
	 	  // user.log is in distributed cache
	 
	 	  //try (BufferedReader br = new BufferedReader(new FileReader("ratings.dat"))) {
	 		
	 	     
	 	   try 
	 	   {
	 		   
	 		  
		  Path pt=new Path("hdfs:/user/guru/ratings.dat");//Location of file in HDFS
		  FileSystem fs = FileSystem.get(new Configuration());
	 		   
	 	   BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt))); 
	 	  // List<UserActivityVO> userList = new ArrayList<UserActivityVO>();
	 
	 	   String line;
	 
	 	   while ((line = br.readLine()) != null) {
	 		  user = new UserActivityVO();
	 
	 	    String columns[] = line.split("::",-1);
	 	    
	 	   
	 
	 	    
/*	 	    user.setUserId(Integer.parseInt(columns[0])); //userid
			user.setMovieId(columns[1]); 				  //movieid
			//user.setRating(Integer.parseInt(columns[2])); //rating
			
			if(columns[1].equals(user.getMovieId()))
			{
				userList.add(user);
			}
			else
			{			
			   userMap.put(columns[1], userList);
			}
			
			if(userMap.get(columns[1])!=null)
			{
				userList = userMap.get(columns[1]);
				userList.add(user);
				
			}
			else
			{			
			   userMap.put(columns[1], userList);
			}*/
	 
	 	   }
	 
	 	  } catch (IOException e) {
	 
	 	   e.printStackTrace();
	 
	 	  }	 	 
	 
	 	 }
}