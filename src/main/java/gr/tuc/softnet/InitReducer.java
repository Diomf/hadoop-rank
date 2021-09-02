package gr.tuc.softnet;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String k = key.toString(); 
		int rank = 1; 
		
		String out ="";
		for (Text value : values) // iterate those followed by k
		{
			out = out + value.toString() + ","; 
		}
		
        k = k + ":" + rank; 
		out = out.substring(0, out.lastIndexOf(',')); 
		context.write(new Text(k), new Text(out)); //write output in key:rank and followed list

	}
}

