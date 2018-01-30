package com.dataflair.bd.proc.avg;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//18-35::scientist::3444::3
public class UsersRatingDataMapper extends Mapper<Object, Text, Text, Text> {

private Text movieId = new Text();
private Text outvalue = new Text();

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
String data = values.toString();
String[] field = data.split("::", -1);
if (null != field && field.length == 4) {
movieId.set(field[2]);
outvalue.set("C" + field[0] + "::" + field[1]+"::"+field[3]);
context.write(movieId, outvalue);

}

}

}