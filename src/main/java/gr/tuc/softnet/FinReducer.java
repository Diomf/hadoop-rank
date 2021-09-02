package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
// data is organized based on ranks
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		Double rank = -Double.parseDouble(key.toString());
		for (Text v : values) 
		{
			context.write(v, new Text(rank + ""));
		}

	}
}

