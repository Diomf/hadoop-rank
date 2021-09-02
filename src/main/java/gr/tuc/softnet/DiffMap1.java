package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString();
		String[] parts = line.split("\t");
		if (parts.length > 2)
		{
			throw new IOException("Incorrect data format");
		}
		String[] key_rank = parts[0].split(":"); 

		context.write(new Text(key_rank[0]), new Text(key_rank[1]));

	}

}
