package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); 
		String[] sections = line.split("\t");
		if (sections.length > 2)
		{
			throw new IOException("Incorrect data format");
		}
		String[] parts = sections[0].split(":");
		Double rank = Double.parseDouble(parts[1]);
		//Pass rank as key and exploit hadoop's sort between map and reduce
		//Change sign for descending sorting and fix it back in reducer
		context.write(new DoubleWritable(-rank), new Text(parts[0])); 

	}

}
