package gr.tuc.softnet;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		int i = 0;
		for (Text v : values) { 
			// must be 2 ranks for every key
			if (i < 2) { 
				ranks[i] = Double.parseDouble(v.toString());
			} else {
				throw new IOException("Found more than 2 ranks");
			}
			i++;
		}

		double difference = Math.abs(ranks[0] - ranks[1]);
		context.write(new Text(difference + ""), new Text());

	}
}
