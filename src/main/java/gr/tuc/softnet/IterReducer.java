package gr.tuc.softnet;

import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.math.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//Data is coming as format: key followedlist or key weight
		
		String followedlist="";
		double sum_weight = 0.0;
		for (Text v : values) //Iterate values
		{   
			String tmp=v.toString();    
			if (!tmp.contains("!")) {      //If it's weight, add it to the total as shown in the assignment
				double weight = Double.parseDouble(tmp); 
				sum_weight = sum_weight + (weight); 
			} else {                    //or keep the followed list
				followedlist = tmp.substring(1);       //without the '!'
			}
		}
		
		double rank = 0.0; //create the new rank
		rank = 0.15 + (0.85) * sum_weight;   //Assignment's equation for rank

		context.write(new Text(key.toString() + ":" + round(rank,3)), new Text(followedlist)); //write the new key:rank and its followed list

	}
	
	public static double round(double value, int places) {  
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
}
