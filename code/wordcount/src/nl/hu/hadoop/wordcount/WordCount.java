package nl.hu.hadoop.wordcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.IntegerSplitter;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(MyWriteable.class);

		job.waitForCompletion(true);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, IntWritable, MyWriteable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {

		Log log = LogFactory.getLog(WordCountMapper.class);

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

        while (itr.hasMoreTokens()) {

            IntWritable intValue = new IntWritable();
			intValue.set(Integer.parseInt(itr.nextToken()));

            int sum = 0;

            for(int i = 1; i < intValue.get(); i++) {
                if((intValue.get() % i) == 0) {
                    sum += i;
                }
            }


			//log.info("origineel: " + intValue.toString() + " en bevriend is: " + sum);

			IntWritable s = new IntWritable(sum);

			context.write(s, new MyWriteable(intValue, s));

			if(intValue != s) {
				context.write(intValue, new MyWriteable(intValue, s));
			}
        }
	}
}

class WordCountReducer extends Reducer<IntWritable, MyWriteable, IntWritable, IntWritable> {
	public void reduce(IntWritable key, Iterable<MyWriteable> values, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();

		int i = 0;
		int a = 0;

		HashMap<Integer, Integer> m = new HashMap<>();
		for(MyWriteable val : values) {

			m.put(new Integer(val.getOn().toString()), new Integer(val.getAn().toString()));
			i++;

			sb.append(i + " " + val + " ");


		}

		Integer thisAn = m.get(new Integer(key.toString()));
		Integer thisOn = new Integer(key.toString());

		if(m.get(thisAn) != null)
		{
			if(m.get(thisAn).equals(thisOn) && !thisOn.equals(thisAn)) {
				context.write(key, new IntWritable(m.get(thisOn)));
			}
		}

	}
}

class MyWriteable implements WritableComparable<MyWriteable> {
	private IntWritable on;
	private IntWritable an;

	public MyWriteable() {
		set(new IntWritable(), new IntWritable());
	}

	public MyWriteable(MyWriteable p) {
		set(new IntWritable(p.on.get()), new IntWritable(p.an.get()));
	}
	public MyWriteable(IntWritable on, IntWritable an) {
		set(on, an);
	}
	public MyWriteable(int o, int t) {
		set(new IntWritable(o), new IntWritable(t));
	}

	public void set(IntWritable input, IntWritable sumFactors) {
		this.on = input;
		this.an = sumFactors;
	}

	public IntWritable getOn() {
		return this.on;
	}

	public IntWritable getAn() {
		return this.an;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		on.write(out);
		an.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		on.readFields(in);
		an.readFields(in);
	}

	@Override
	public int hashCode() {
		return on.hashCode() * 163 + an.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof MyWriteable) {
			MyWriteable tp = (MyWriteable) o;
			return on.equals(tp.on) && an.equals(tp.an);
		}
		return false;
	}

	@Override
	public String toString() {
		return "ON:" + on + " AN:" + an;
	}

	public void setRaw(MyWriteable p) {
		this.on.set(p.getOn().get());
		this.an.set(p.getAn().get());
	}

	@Override
	public int compareTo(MyWriteable tp) {
		// int cmp = input.compareTo(tp.input);
		// if(cmp!=0){
		//   return cmp;
		// }
		// Sort on sumFactors
		return on.compareTo(tp.on);
	}
}
