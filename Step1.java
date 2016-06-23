
import java.io.IOException;
// import java.io.InputStreamReader;
// import java.io.Reader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Step1 {
	

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String song_no = "";
//			String time_table ="";
//			String access_route = "";
			String customer_no ="";
			
			StringBuilder songncustomer = new StringBuilder();
			
			String subtext = "";
			int n = 0;

			subtext = line.substring(0, line.length());
			// System.out.println(subtext);

			for (int i = 0; i < subtext.length(); i++) {
				if (subtext.charAt(i) != '|') {
					continue;
				}

				n++;
			}
			// System.out.println(n);

			if (n != 3) {
				return;
			}

			StringTokenizer tokenizer = new StringTokenizer(line, "|");

			try {

				while (tokenizer.hasMoreTokens()) {
					song_no = tokenizer.nextToken();
//					word.set(song_no);
//					context.write(word, one);
//					timetable = tokenizer.nextToken();
//					access_route = tokenizer.nextToken();
					tokenizer.nextToken();
					tokenizer.nextToken();
					customer_no = tokenizer.nextToken();
					songncustomer.append(song_no);
					songncustomer.append("@");
					songncustomer.append(customer_no);
					word.set(songncustomer.toString());
					context.write(word, one);
					songncustomer.setLength(0);
				}

			} catch (Exception e) {
				// e.printStackTrace();
				return;
			}
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable sumWritable = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			try {

				long sum = 0;
				for (LongWritable val : values) {
					sum += val.get();
				}

				sumWritable.set(sum);
				context.write(key, sumWritable);
			} catch (Exception e) {
				// e.printStackTrace();
				long sum = 0;
				for (LongWritable val : values) {
					sum += val.get();
				}

				sumWritable.set(sum);
				context.write(key, sumWritable);

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapred.textoutputformat.separator", "@");
		
		Job job = new Job(conf, "WordCount");

		job.setJarByClass(Step1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// An InputFormat for plain text files. Files are broken into lines.
		// Either linefeed or carriage-return are used to signal end of
		// Keys are the position in the file, and values are the line of text..
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
