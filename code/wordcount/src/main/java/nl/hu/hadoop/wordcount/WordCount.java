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
		String[] words = value.toString().split("\\s");

		String[] alphabet = { "a", "b", "c", "d", "e", "f", "g",
				"h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
				"u", "v", "w", "x", "y", "z" };

		int wordCounter = 0;

		for (String word : words) {
			wordCounter += 1;
			String MyWord = word.replaceAll("[^a-zA-Z]", "");
			MyWord = MyWord.toLowerCase();

			char[] MyWordCharacters = MyWord.toCharArray();

			int characterCount = MyWordCharacters.length;

			Log log = LogFactory.getLog(WordCountMapper.class);
			log.info("word: '" + MyWord + "' and " + characterCount);

			for(int i = 0; i < characterCount; i++) {
				if(i != (characterCount-1)) {
					char letterErna = MyWordCharacters[(i + 1)];


					StringBuilder sb = new StringBuilder();

					int j = 0;
					for (String letter : alphabet) {
						if (j != 0) {
							sb.append(",");
						}
						if (letter.equals(Character.toString(letterErna))) {
							sb.append("1");
						} else {
							sb.append("0");
						}
						j += 1;
					}

					context.write(new Text(Character.toString(MyWordCharacters[i])), new Text(sb.toString()));
				}
			}
		}
	}
}

class WordCountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Log log = LogFactory.getLog(WordCountMapper.class);
		String[] alphabet = { "a", "b", "c", "d", "e", "f", "g",
				"h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t",
				"u", "v", "w", "x", "y", "z" };
		if (key.equals(new Text("a"))) {
			context.write(new Text("Letter, "), new Text(String.join(",", alphabet)));
		}
		Integer[] countArray = new Integer[26];
		Arrays.fill(countArray, 0);

		StringBuilder sb = new StringBuilder();
		for (Text i : values ) {
			String[] numbers = i.toString().split(",");
			for(int k = 0; k < numbers.length; k++) {
				if (numbers[k].toString().equals("1")) {
					countArray[k] += 1;
				}
			}
		}
		int l = 0;
		for(Integer c : countArray) {
			if(l != 0) {
				sb.append(',');
			}
			sb.append(c);
			l += 1;
		}

		context.write(new Text(key + ","), new Text(sb.toString()));
	}
}
