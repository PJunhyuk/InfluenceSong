
import java.io.IOException;
// import java.io.InputStreamReader;
// import java.io.Reader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Step4 {
	
	public static class MyMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text customer_no = new Text();
		private Text tfidfnsong = new Text();

		public MyMapper() {
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] devidebytab = value.toString().split("\t");
			String[] devidebygolbange = devidebytab[0].split("@");
			String[] devidebybar = devidebytab[1].toString().split("|");
			
			this.customer_no.set(new Text(devidebygolbange[0]));
			this.tfidfnsong.set(devidebygolbange[1] + "@" + devidebybar[0]);
			context.write(this.customer_no, this.tfidfnsong);				
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private Text customer_no = new Text();
		private Text influencesong = new Text();
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			double max = -1d;
			for (Text val : values) {
				String[] tfidfAndDoc = val.toString().split("@");
				if(Double.valueOf(tfidfAndDoc[0]) > max) {
					max = Double.valueOf(tfidfAndDoc[0]);
					this.customer_no.set(key);
					this.influencesong.set(val.toString().substring(0, 9));
				}
			}
			context.write(this.customer_no, this.influencesong);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapred.textoutputformat.separator", "@");
		
		Job job = new Job(conf, "WordCount");

		job.setJarByClass(Step4.class);
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
