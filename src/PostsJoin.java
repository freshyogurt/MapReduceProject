import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class PostsJoin {

	public static class PostsMapper extends Mapper<Object, Text, Text, Text> {

		private Text outkey = new Text();
		private Text outvalue = new Text();

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input line into a string array
			String[] line = new CSVParser().parseLine(value.toString());

			// Only keep tuples whose post date are between 2009 and 2013
			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"yyyy-MM-dd'T'HH:mm:ss.SSS");
			Date parsedDate = null;
			try {
				parsedDate = dateFormat.parse(line[4]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			SimpleDateFormat df = new SimpleDateFormat("yyyy");
			String yearStr = df.format(parsedDate);
			int year = Integer.parseInt(yearStr);
			if (year >= 2009 && year <= 2013) {
				if (line[1].equals("1")) {

					// For each question, the foreign join key is (Id)
					outkey.set(line[0]);

					// Flag this record for the reducer and then output
					// attributes Id and CreationDate
					outvalue.set("Q" + line[0] + "," + line[4]);
					context.write(outkey, outvalue);

				} else if (line[1].equals("2")) {

					// For each answer, the foreign join key is (ParentID)
					outkey.set(line[2]);

					// Flag this record for the reducer and then output
					// attributes CreationDate
					outvalue.set("A" + line[4]);
					context.write(outkey, outvalue);
				}
			}
		}
	}

	public static class QAJoinReducer extends
			Reducer<Text, Text, NullWritable, Text> {

		private ArrayList<Text> listQ = new ArrayList<Text>();
		private ArrayList<Text> listA = new ArrayList<Text>();		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			// Clear our lists
			listQ.clear();
			listA.clear();

			// Iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			Date minDate = null, currentDate = null;
			String dateStr = null;
			for (Text t : values) {
				if (t.charAt(0) == 'Q') {
					listQ.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'A') {
					try {
						currentDate = dateFormat.parse(t.toString()
								.substring(1));
						if(minDate == null || minDate.compareTo(currentDate) > 0) {
							minDate = currentDate;
							dateStr = t.toString().substring(1);
						}
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if(dateStr != null)
				listA.add(new Text(dateStr));

			// If both lists are not empty, join Q with A
			if (!listQ.isEmpty() && !listA.isEmpty()) {
				for (Text Q : listQ) {
					for (Text A : listA) {
						String[] lineQ = new CSVParser()
								.parseLine(Q.toString());

						Date parsedDate = null;
						try {
							parsedDate = dateFormat.parse(lineQ[1]);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						long diff = 0;
						try {
							diff = (dateFormat.parse(A.toString()).getTime() - parsedDate
									.getTime()) / 1000;
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						context.write(null, new Text(lineQ[0] + "," + lineQ[1]
								+ "," + diff));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: JoinPosts <in> <out>");
			System.exit(1);
		}

		Job job = new Job(conf, "Join Posts");

		job.setJarByClass(PostsJoin.class);
//		job.setNumReduceTasks(5);

		job.setMapperClass(PostsMapper.class);
		job.setReducerClass(QAJoinReducer.class);

		// the map output is Text, Text
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// the reduce output is NullWritable, Text
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 3);
	}
}