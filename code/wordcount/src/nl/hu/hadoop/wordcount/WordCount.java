package nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(WordCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

	class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		Log log = LogFactory.getLog(WordCountMapper.class);

		String[] alphabet = { "a", "b", "c", "d", "e", "f", "g",
				"h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
				"u", "v", "w", "x", "y", "z" };

		int lineN = 0;

		String val = value.toString();
		String[] parts = val.split(",");

		log.info(parts[0]);
		if(!parts[0].equals("Letter")) {
			int xTotal = 0;
			int i = 0;
			for(String part : parts) {
				if(i != 0) {
					//log.info("adding " + Integer.parseInt(part.trim()) + " to " + xTotal);
					context.write(new Text(alphabet[i-1]), new IntWritable(Integer.parseInt(part.trim())));
					//xTotal += Integer.parseInt(part.trim());
				}
				i += 1;
			}

		}
		lineN += 1;

	}
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		System.out.println("KEY: " + key);
		System.out.println(("VALUES: "));


		String[] alphabet = { "a", "b", "c", "d", "e", "f", "g",
				"h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
				"u", "v", "w", "x", "y", "z" };

		Integer total = 0;
		for(IntWritable val : values) {
			total += new Integer(val.toString());
		}
		context.write(key, new Text(total.toString()));

	}
}
