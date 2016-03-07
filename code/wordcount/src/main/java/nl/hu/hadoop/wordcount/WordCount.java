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
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}

	class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		Log log = LogFactory.getLog(WordCountMapper.class);

		int lineN = 0;

		String[] parts = line.split(",");

		log.info(parts[0]);
		if(!parts[0].equals("Letter")) {
			log.info(parts[0] + " DOESNT EQUAL LETTER");
			int xTotal = 0;
			int i = 0;
			for(String part : parts) {
				if(i != 0) {
					log.info("adding " + Integer.parseInt(part.trim()) + " to " + xTotal);
					xTotal += Integer.parseInt(part.trim());
				}
				i += 1;
			}
			log.info("GOT AS XTOTAL: " + xTotal);
		}
		log.info("line: " + lineN);
		lineN += 1;
		log.info("HUUH LINE: " + lineN);

	}
}

class WordCountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	}
}
