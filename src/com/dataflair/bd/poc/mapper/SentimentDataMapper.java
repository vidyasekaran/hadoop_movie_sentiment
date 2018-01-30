package com.dataflair.bd.poc.mapper;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;



public class SentimentDataMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException 
	{    
	    
		
		String line = value.toString();
		String[] singleLine = line.split(":");
		context.write(new Text(key), new Text(singleLine[1]));
		
		/*for (int i = 0; i < singleLine.length; i++) 
		{
			 if(singleLine[i].contains("text") )
	         {
				 //context.write(null,new Text(singleLine[i]));
				 
				 String[] lines = singleLine[i].split(":");
				 
				 for (int j = 0; j < lines.length; j++) 
				 {
					    Map<String, String> map = Splitter.on( ":" ).withKeyValueSeparator( "," ).split( lines[j] );
						
					    
						String id = map.get("id_str");
						String text_value = map.get("text");
						
						if(id!=null && text_value!=null)
						context.write(new Text(id), new Text(text_value));
					
				 }
				 
	         }
		}*/
		
		
		//Map<String, String> map = Splitter.on( ":" ).withKeyValueSeparator( "," ).split( line );
		
		//String id = map.get("id_str");
		//String text_value = map.get("text");
		
		
		//
		
	//	tuple.split(":");
		
		
		
		//context.write(new Text(key), new Text(value));
		
     /* StringTokenizer tokenizer = new StringTokenizer(line,":");
        
       StringBuffer buf = new StringBuffer();
        
     //context.write(new Text(key), new Text(line));
        
        while (tokenizer.hasMoreTokens()) 
        {
         String token = tokenizer.nextToken();
         
         if(token.contains("text")|| token.contains("full_text") )
         {
        	 
        	 buf.append(token);
        	 if(!token.contains("profile_text_color"))
        	 {
        		 context.write(new Text(key), new Text(token));
        	 }
        	 
        			 
        	 
         // Context here is like a multi set which allocates value "one" for key "word".
        // context.write(new Text(token.toLowerCase()), new Text("1"));
         
        }*/
        
        
        
       /* String[] tuple = line.split("\\n");
        JSONParser jsonParser = new JSONParser();
         
        try{
         
        for(int i=0;i<tuple.length; i++){
         
        JSONObject obj =(JSONObject) jsonParser.parse(line);
         
        String tweet_id = (String) obj.get("id_str");
         
        String tweet_text=(String) obj.get("text");
         
        context.write(new Text(tweet_id),new Text(tweet_id));
         
        }
         
        }catch(Exception e){
         
        e.printStackTrace();*/
       
		
	//}

}
}