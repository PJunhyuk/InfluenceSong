
import java.io.IOException;
import java.text.DecimalFormat;
// import java.io.InputStreamReader;
// import java.io.Reader;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Step3 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text songncustomer = new Text();
		private Text customernratio = new Text();
		
		public MyMapper() {
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] customernratio = value.toString().split("\t");
			String[] songncustomer = customernratio[0].split("@");
			this.songncustomer.set(new Text(songncustomer[0]));
			this.customernratio.set(songncustomer[1] + "=" + customernratio[1]);
			context.write(this.songncustomer, this.customernratio);
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private static final DecimalFormat DF = new DecimalFormat("###.########");
		
		private Text songncustomer = new Text();
		private Text tfidfvalue = new Text();		
		
		public MyReducer() {
		}
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int totalcustomercount = 0;

			int customerwholistenthatsong = 0;
			
			Map<String, String> temp = new HashMap<String, String>();
			for (Text val : values) {
				String[] songncustomerandratio = val.toString().split("=");
				if (Integer.parseInt(songncustomerandratio[1].split("/")[0]) > 0) {
					customerwholistenthatsong++;
				}
				temp.put(songncustomerandratio[0], songncustomerandratio[1]);
			}
			for (String document : temp.keySet()) {
				totalcustomercount += customerwholistenthatsong;
			}
			
			for (String document : temp.keySet()) {
				String[] ratioset = temp.get(document).split("/");
				
				double tf = Double.valueOf(Double.valueOf(ratioset[0]) / Double.valueOf(ratioset[1]));
				double idf = Math.log10((double)totalcustomercount / (double)((customerwholistenthatsong == 0 ? 1 : 0) + customerwholistenthatsong));
				
				double tfidf = tf * idf;
				
				this.songncustomer.set(document + "@" + key);
				this.tfidfvalue.set(DF.format(tfidf) + "|" + ratioset[0] + "|" + ratioset[1] + "|" + totalcustomercount + "|" + customerwholistenthatsong);
				
				context.write(this.songncustomer, this.tfidfvalue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapred.textoutputformat.separator", "@");
		
		Job job = new Job(conf, "WordCount");

		job.setJarByClass(Step3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		// if mapper outputs are different, call setMapOutputKeyClass and
		// setMapOutputValueClass
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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
