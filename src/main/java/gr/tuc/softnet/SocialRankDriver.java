package gr.tuc.softnet;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SocialRankDriver {
	public static void main(String[] args) throws Exception {
		

		String job = "";
		if (args.length != 0) //Argument parsing 
			job = args[0];
		if (args.length == 4) //For functions with 4 arguments
		{

			if (job.equals("init")) //Check function by looking up the first argument
			{
				init(args[1], args[2], Integer.parseInt(args[3]));
			} else if (job.equals("iter")) {
				iter(args[1], args[2], Integer.parseInt(args[3]));
			} else if (job.equals("finish")) {
				finish(args[1], args[2], Integer.parseInt(args[3]));				
			} else //Wrong function
			{
				System.err
				.println("Wrong function name");
			}
		} else if (args.length == 5) //For function with 5 arguments
		{
			if (job.equals("diff"))
			{
				diff(args[1], args[2], args[3], Integer.parseInt(args[4])); 
			} else
			{
				System.err
				.println("Wrong function name");
			}
		} else if (args.length == 8) // For the composite function with 8 arguments
		{
			if (job.equals("composite")) {
				composite(args[1], args[2], args[3], args[4], args[5],
						Double.parseDouble(args[6]),Integer.parseInt(args[7]));
			} else 
			{
				System.err
				.println("Please check the name of the function you wish to call and try again");
			}
		} else {
			System.err
			.println("Incorrect Usage \n Correct format: <function name><input><output><#reducers> \n Or \n <function name><input><output><diff><#reducers>"
					+ "\n Or \n <function name><input><output><interim1><interim2><diff><#reducers>");
		}
	}

	static void init(String input, String output, int reducers)
			throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Beginning Init:");
		Job job = Job.getInstance(); 
		job.setJarByClass(SocialRankDriver.class); // Set the running jar
		job.setNumReduceTasks(reducers); // Define number of reducers

		FileInputFormat.addInputPath(job, new Path(input)); 
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(InitMapper.class); // Set mappers and reducers
		job.setReducerClass(InitReducer.class);

		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
       
		//Run the job, wait and print corresponding message 
		System.out.print(job.waitForCompletion(true) ? "Init Job Completed" : "Init Job Error");
	}

	static void iter(String input, String output, int reducers)
			throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("Beginning Iter:");
		Job job = Job.getInstance(); 
		job.setJarByClass(SocialRankDriver.class); 
		job.setNumReduceTasks(reducers);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(IterMapper.class); 
		job.setReducerClass(IterReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.out.print(job.waitForCompletion(true) ? "Iter Job Completed"
				: "Iter Job Error");

	}
	
	static void diff(String input1, String input2, String output, int reducers)
			throws Exception {
		System.out.println("Beginning Diff-1");
		Job job = Job.getInstance(); 
		job.setJarByClass(SocialRankDriver.class); 
		job.setNumReduceTasks(reducers); 

		FileInputFormat.addInputPath(job, new Path(input1)); //input from intemediate output
		FileInputFormat.addInputPath(job, new Path(input2)); 
		FileOutputFormat.setOutputPath(job, new Path("tempdiff"));

		job.setMapperClass(DiffMap1.class); // First job
		job.setReducerClass(DiffRed1.class);

		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if (job.waitForCompletion(true)) // Run the second job as soon as the first one finishes
		{
			System.out.println("Beginning Diff-2");
			Job job1 = Job.getInstance(); 
			job1.setJarByClass(SocialRankDriver.class); 
			job1.setNumReduceTasks(reducers);

			FileInputFormat.addInputPath(job1, new Path("tempdiff")); //Pass as input the temporary output that we created
			FileOutputFormat.setOutputPath(job1, new Path(output)); 

			job1.setMapperClass(DiffMap2.class);
			job1.setReducerClass(DiffRed2.class);

			job1.setMapOutputKeyClass(Text.class); 
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			System.out
			.print(job1.waitForCompletion(true) ? "Diff Job Completed"
					: "Diff Job Error");
			deleteDirectory("tempdiff"); //Delete temp
		}

	}
	
	static void finish(String input, String output, int reducers)
			throws Exception {
		System.out.println("Beginning finish");
		Job job = Job.getInstance(); 
		job.setJarByClass(SocialRankDriver.class); 
		job.setNumReduceTasks(reducers); 

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(FinMapper.class); 
		job.setReducerClass(FinReducer.class);

		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.out.print(job.waitForCompletion(true) ? "Finish Job Completed"
				: "Finish Job Error");
		deleteDirectory(input);

	}
	
	public static void composite(String input, String output, String intermed1,
			String intermed2, String diff, double threshold, int reducers) throws Exception {
		
		init(input, intermed1, reducers); //Start the first intermediate job
		double difference = 100000000; //Set an initial high value for the convergence check
		int i = 0; 
		while (difference >= threshold) 
		{
			if (i % 2 == 0) {
				iter(intermed1, intermed2, reducers); //For even loops, set intermed1 as input and intermed2 as intermediate output
				                                      
			} else {
				iter(intermed2, intermed1, reducers); //For odd loops, swap the previous directories
			}
			if (i % 3 == 0) // Calculate rank change every 3 loops
			{
				diff(intermed1, intermed2, "diffout", reducers); //Calculate absolute difference of rank in intermed1 and 2
				difference = readDiffResult("diffout"); 
				System.out.println("Difference updates to:" + difference);
				deleteDirectory("diffout"); // Delete temp directory
			}

			if (i % 2 == 0) //In even loops, delete intermed1(input)
			{               
				deleteDirectory(intermed1);
			}
			if (i % 2 == 1) //In odd, delete intermed2
			{
				deleteDirectory(intermed2);
			}
			i++; 
		}

		if (i % 2 == 1) //Check if while ended on odd or even number to define the input of finish job
		{
			deleteDirectory(intermed1);
			finish(intermed2, output, reducers);
			summarizeResult(output);
		} else
		{
			deleteDirectory(intermed2);
			finish(intermed1, output, reducers);
			summarizeResult(output);
		}

	}

	//Check output files, keep the 10 first values on each one, sort them and write them in an output file
	static void summarizeResult(String path) throws Exception {
		Path finpath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		HashMap<Long, Double> values = new HashMap<Long, Double>(); //Store keys-ranks in hashmap
		int size = 0;

		if (fs.exists(finpath)) {
			FileStatus[] ls = fs.listStatus(finpath);
			for (FileStatus file : ls) {                  //Loop over output file
				if (file.getPath().getName().startsWith("part-r-00")) {
					FSDataInputStream diffin = fs.open(file.getPath());  
					BufferedReader d = new BufferedReader(              
							new InputStreamReader(diffin));             
					int i = 0;
					String diffcontent = "x"; 
					
					while (i <= 10 && diffcontent != null) 
					{
						diffcontent = d.readLine();        
						if (diffcontent != null) 
						{
							String[] parts = diffcontent.split("\t");
							long node = Long.parseLong(parts[0]);
							double rank = Double.parseDouble(parts[1]);
							values.put(node, rank);
							i++;
							size++; 
						}
					}
					d.close();
				}
			}
			Long[] nodes = new Long[size];
			Double[] ranks = new Double[size];
			int j = 0;
			for (Map.Entry<Long, Double> entry : values.entrySet())//Iterate hashmap and store keys-ranks in arrays
			{
				nodes[j] = entry.getKey();
				ranks[j] = entry.getValue();
				j++;
			}

			for (int i = 0; i < j - 1; i++)      //Sorting
			{
				for (int k = i + 1; k < j; k++) {
					if (ranks[i] < ranks[k]) {
						double temp = ranks[i]; 
						ranks[i] = ranks[k];
						ranks[k] = temp;

						Long temps = nodes[i]; 
						nodes[i] = nodes[k];
						nodes[k] = temps;
					}
				}
			}

			try {
				OutputStream os = fs.create(new Path(path + "/output.txt")); 

				for (int i = 0; i < nodes.length; i++) {
					String out = nodes[i] + "\t" + ranks[i] + "\n"; 
					for (int k = 0; k < out.length(); k++) {
						char c = out.charAt(k);
						os.write(c);
					}
				}
				os.close(); 
			} catch (IOException e) {
				System.out.println("Any Errors:");
				e.printStackTrace();
			}
		}
		System.out.println();
		System.out.println("Results Summarized");

		fs.close();
	}

	//Get result from diffs for convergence check
	static double readDiffResult(String path) throws Exception {
		double diffnum = 0.0;
		Path diffpath = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(diffpath)) {
			FileStatus[] ls = fs.listStatus(diffpath);
			for (FileStatus file : ls) { 
				
				if (file.getPath().getName().startsWith("part-r-00")) {
					FSDataInputStream diffin = fs.open(file.getPath());
					BufferedReader d = new BufferedReader(
							new InputStreamReader(diffin));
					String diffcontent = d.readLine();
					if (diffcontent != null) {
						double diff_temp = Double.parseDouble(diffcontent);
						if (diffnum < diff_temp) {
							diffnum = diff_temp;
						}
						d.close();
					}
				}
			}
		}

		fs.close();
		return diffnum;
	}

	static void deleteDirectory(String path) throws Exception {   
		Path todelete = new Path(path);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);

		if (fs.exists(todelete))
			fs.delete(todelete, true);

		fs.close();
	}

}
