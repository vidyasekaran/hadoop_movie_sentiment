package com.dataflair.bd.proc.avg;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingDataMapper extends Mapper<Object, Text, Text, Text> 
{

	
	//Input Data - ratings): UserID::MovieID::Rating::Timestamp ==> 1::1193::5::978300760
	
private Text userId = new Text();
private Text outvalue = new Text();

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
String data = values.toString();
String[] field = data.split("::", -1);
if (null != field && field.length == 4 && field[0].length() > 0) {

userId.set(field[0]);
outvalue.set("R" + field[1]+"::"+field[2]);
context.write(userId, outvalue);

}

}

}