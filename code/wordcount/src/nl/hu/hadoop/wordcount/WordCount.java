package nl.hu.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.StringTokenizer;

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
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            StringTokenizer itr2 = new StringTokenizer(itr.nextToken());
            String website = itr2.nextToken();
            Text websiteText = new Text();
            websiteText.set(website);
            while (itr2.hasMoreTokens()) {
                Text keywordText = new Text(itr2.nextToken());
                context.write(keywordText, websiteText);
            }
        }
	}
}

class WordCountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder output = new StringBuilder();
        for(Text value : values) {
            output.append(value.toString() + " ");
        }
        Text result = new Text();
        result.set(output.toString());
		context.write(key, result);
	}
}
