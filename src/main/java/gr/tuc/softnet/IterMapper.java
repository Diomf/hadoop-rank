package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
	IllegalArgumentException {
		String line = value.toString(); 
		String[] parts = line.split("\t"); //split key:rank and followed users
		if (parts.length > 2) // Check data format
		{
			throw new IOException("Wrong data format");
		}
		if (parts.length != 2) {
			return;
		}
		String[] follows = parts[1].split(","); 
		String[] key_rank = parts[0].split(":"); 
		double rank = Double.parseDouble(key_rank[1]); 
		int number = follows.length; 

		double weight = (rank / number); //  Rj/N(j)

		String val = weight + ""; 
		for (int i = 0; i < number; i++) // write followed users and his rank/number
			
		{
			context.write(new Text(follows[i]), new Text(val));
		}

		context.write(new Text(key_rank[0]), new Text("!" + parts[1])); //write key and followed users	

	}

}
