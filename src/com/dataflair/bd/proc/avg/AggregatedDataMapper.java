package com.dataflair.bd.proc.avg;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//Crime::18-35::other or not specified::2

//key -> other or not specified::2 , value ->Crime::2
public class AggregatedDataMapper extends Mapper<Object, Text, Text, Text> {

private Text keyData = new Text();
private Text outvalue = new Text();

@Override
public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
String data = values.toString();
String[] field = data.split("::", -1);
if (null != field && field.length == 4) {
keyData.set(field[2] + "::" + field[1]);
outvalue.set(field[0] + "::" + field[3]);
context.write(keyData, outvalue);

}

}

}