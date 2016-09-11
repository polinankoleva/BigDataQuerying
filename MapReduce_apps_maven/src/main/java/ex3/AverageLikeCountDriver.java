package ex3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ex2b.LikeCountMapper;
import ex2b.LikeCountReducer;

/**
 * The main point of the program for counting the average number of likes per
 * user. We determine the input and output folders from the input
 * parameters. Moreover, we have used a third folder which is used as a
 * temporary one. For finding the average number of likes per user, we have two
 * jobs running one after another. The first one computes the total number of
 * likes per user. The user mapper class is {@link LikeCountMapper} and
 * respectively the reducer - {@link LikeCountReducer}. This job used the file
 * from the given input folder and saves its result file in our specific
 * temporary folder. This file is used as an input for our second job. Having a
 * list of users and the number of likes, the second job does sum over all likes
 * and count all user. Finally, to receive the average number of likes per user,
 * we just divide the total likes number by the total users number. The second
 * job uses as a mapper - {@link AverageLikeCountMapper} and as a reducer
 * {@link AverageLikeCountReducer}. Additionally, the configuration of a job ,
 * setting of mapper and reducer, their key/values types are done in this class
 * as well. For more information see the documentation of used mapper/reducer
 * classes.
 * 
 * @author Polina Koleva
 *
 */
public class AverageLikeCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate that four arguments were passed from the command line.
		 */
		if (args.length != 4) {
			System.out
					.printf("Usage: AverageLikeCountDriver <input dir> <temp dir> <output dir> <num of reducers>\n");
			System.exit(-1);
		}

		// Configuration processed by ToolRunner
		Configuration conf = getConf();

		// Notify Hadoop that application uses GenericOptionsParser
		// This is not required but prevents that a warning is printed during
		// execution
		conf.set("mapreduce.client.genericoptionsparser.used", "true");

		// Create a Job using the processed conf
		Job firstJob = Job.getInstance(conf);

		// Define Input and Output Format
		firstJob.setInputFormatClass(TextInputFormat.class);
		firstJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(firstJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		firstJob.setMapOutputKeyClass(Text.class);
		firstJob.setMapOutputValueClass(IntWritable.class);

		// Define Job Output Classes (Key, Value)
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(IntWritable.class);

		// Set Mapper and Reducer Class
		firstJob.setMapperClass(LikeCountMapper.class);
		firstJob.setReducerClass(LikeCountReducer.class);
		// For LikeCount we can use the Reducer class also as Combiner
		// class
		// do the same as reducer but on the local level
		// so finally we will do the reduce step twice
		firstJob.setCombinerClass(LikeCountReducer.class);

		// Set the Number of Reduce Tasks
		firstJob.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		firstJob.setJarByClass(AverageLikeCountDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		firstJob.setJobName("LikeCount");
		firstJob.waitForCompletion(true);

		// Second job
		// While the first job is to retrieve the number of likes per user, the
		// second job
		// is using the output of the first job to calculate the average number
		// of likes per user.
		// Create a Job using the processed conf
		Job secondJob = Job.getInstance(conf);

		// Define Input and Output Format
		secondJob.setInputFormatClass(TextInputFormat.class);
		secondJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(secondJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		secondJob.setMapOutputKeyClass(Text.class);
		secondJob.setMapOutputValueClass(IntWritable.class);

		// Define Job Output Classes (Key, Value)
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(IntWritable.class);

		// Set Mapper and Reducer Class
		secondJob.setMapperClass(AverageLikeCountMapper.class);
		secondJob.setReducerClass(AverageLikeCountReducer.class);

		// Set the Number of Reduce Tasks
		secondJob.setNumReduceTasks(Integer.parseInt(args[3]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[3]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		secondJob.setJarByClass(AverageLikeCountDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		secondJob.setJobName("AverageLikeCount");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return secondJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AverageLikeCountDriver(), args);
		System.exit(exitCode);
	}
}
