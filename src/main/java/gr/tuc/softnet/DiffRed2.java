package gr.tuc.softnet;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {
    
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; 
		int i = 0;
		for (Text v : values) {
			if (i == 0) {
				// Initialize max on the first diff found
				diff_max = Double.parseDouble(v.toString());
				i++;
				continue; //jump to the second loop
			}
			double val = Double.parseDouble(v.toString()); 
			if (val > diff_max) {

				diff_max = val;
			}
		}

		context.write(new Text(diff_max + ""), new Text())

	}
}

