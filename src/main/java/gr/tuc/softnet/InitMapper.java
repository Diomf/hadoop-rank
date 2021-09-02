package gr.tuc.softnet;

import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); 

		if (line.contains("\t")) {   
			String[] parts = line.split("\t"); 
			if (parts[0].matches("[0-9]+") && parts[1].matches("[0-9]+")) { 
				context.write(new Text(parts[0]), new Text(parts[1])); 
			} else {
				throw new IllegalArgumentException("Wrong data format");
			}
		} else {
			throw new IllegalArgumentException("Wrong data format"); 
		}

	}

}

