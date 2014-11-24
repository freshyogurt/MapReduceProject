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

			// Only keep tuples whose post date are in 2008
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date parsedDate = null;
			try {
				parsedDate = dateFormat.parse(line[4]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			SimpleDateFormat df = new SimpleDateFormat("yyyy");
			String year = df.format(parsedDate);
			if (year.equals("2014")) {
				if (line[1].equals("1") && !line[3].isEmpty()) {

					// The foreign join key is (AcceptedAnswerId)
					outkey.set(line[3]);

					// Flag this record for the reducer and then output
					// attributes Id and CreationDate
					outvalue.set("Q" + line[0] + "," + line[4]);
					context.write(outkey, outvalue);

				} else if (line[1].equals("2")) {

					// The foreign join key is (Id)
					outkey.set(line[0]);

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

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Clear our lists
			listQ.clear();
			listA.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			for (Text t : values) {
				if (t.charAt(0) == 'Q') {
					listQ.add(new Text(t.toString().substring(1)));
				} else if (t.charAt(0) == 'A') {
					listA.add(new Text(t.toString().substring(1)));
				}
			}

			// If both lists are not empty, join A with B
			if (!listQ.isEmpty() && !listA.isEmpty()) {
				for (Text Q : listQ) {
					for (Text A : listA) {

						// Filter out those join tuples where the departure time
						// in Flight2 is not after the arrival time in Flights1.
						// Calculate local number of flight records and local
						// total delay.
						String[] lineQ = new CSVParser()
								.parseLine(Q.toString());
						
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						SimpleDateFormat df = new SimpleDateFormat("h");
						Date parsedDate = null;
						try {
							parsedDate = dateFormat.parse(lineQ[1]);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						long diff = 0;
						try {
							diff = (dateFormat.parse(A.toString()).getTime() - parsedDate.getTime())/1000;
							if(diff < 0)
								System.out.println(lineQ[0] + "," + A.toString() + "," + dateFormat.parse(A.toString()) + "," + lineQ[1] + "," + parsedDate.toString());
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						String hour = df.format(parsedDate);
						context.write(null, new Text(lineQ[0] + "," + diff + "," + hour));
						
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
		job.setNumReduceTasks(10);

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
