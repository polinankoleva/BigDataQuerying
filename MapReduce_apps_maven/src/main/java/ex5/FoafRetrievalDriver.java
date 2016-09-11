package ex5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main point of the program for retrieving <all friends of user's friends >. This
 * driver contains two jobs running one after another. The first job does a
 * reduce-side join of social network graph with itself. It takes two input
 * folders where the same data sets are placed. Finally, it produces a data set
 * where for each user there is a list with its friends' friends. The output
 * file is stored in a temporary folder. The mapper for the first job is
 * {@link FirstPhaseFoafRetrievalMapper} and the reducer is
 * {@link FirstPhaseFoafRetrievalReducer}.For more information about this job
 * read their documentation. The second job starts after the first is completed
 * and it uses as an input file of one of its mapper the result file from the
 * first job. This mapper is {@link SecondPhaseSecondFoafRetrievalMapper}.
 * Another mapper part from this job reads again the social network graph -
 * {@link SecondPhaseFirstFoafRetrievalMapper}. The former retrieves all friends
 * of user's friends while the latter all direct friends of an user. Finally,
 * the reducer of the second task {@link SecondPhaseFoafRetrievalReducer} will
 * combine information from the both mappers and will result in final list of
 * indirect user's friend. We determine the two inputs, output and temporary
 * folders which are given as input parameters.
 * 
 * Note: Direct friend of an user means that there is a rdf statement
 * "user foaf:knows userM", so both users are connect only with one edge.
 * Indirect friend means that the user and its indirect friend are connected
 * with two edges "foaf:knows". For example, "user1 foaf:knows user2" and
 * "user2 foaf:knows user3", finally "user3" is indirect friend of "user1".
 * 
 * @author Polina Koleva
 *
 */
public class FoafRetrievalDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate that five arguments were passed from the command line.
		 */
		if (args.length != 5) {
			System.out
					.printf("Usage: FriendsOfTheirFriendsRetrieval <first input dir> <second input dir> <temp dir> <output dir> <num of reducers>\n");
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

		Path firstInputPath = new Path(args[0]);
		Path secondInputPath = new Path(args[1]);
		// input directories
		// we do not set a mapper class because it is a common one for both
		// inputs
		MultipleInputs.addInputPath(firstJob, firstInputPath,
				TextInputFormat.class);
		MultipleInputs.addInputPath(firstJob, secondInputPath,
				TextInputFormat.class);
		// output directory - temporary directory which will be used after from
		// the second job
		FileOutputFormat.setOutputPath(firstJob, new Path(args[2]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		firstJob.setMapOutputKeyClass(CompositeKeyWritable.class);
		firstJob.setMapOutputValueClass(Text.class);

		firstJob.setPartitionerClass(FoafPartitioner.class);
		firstJob.setSortComparatorClass(FoafSortingComparator.class);
		firstJob.setGroupingComparatorClass(FoafGroupingComparator.class);

		// Define Job Output Classes (Key, Value)
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(Text.class);

		// Set Mapper and Reducer Class
		firstJob.setMapperClass(FirstPhaseFoafRetrievalMapper.class);
		firstJob.setReducerClass(FirstPhaseFoafRetrievalReducer.class);

		// Set the Number of Reduce Tasks
		firstJob.setNumReduceTasks(Integer.parseInt(args[4]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[4]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		firstJob.setJarByClass(FoafRetrievalDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		firstJob.setJobName("Foaf retrieval first phase");
		// wait until the first job finishes
		firstJob.waitForCompletion(true);

		// Second job - it takes as an input the output produces by the first
		// job
		// and retrieve the final "friends of user's friends" list
		// Create a Job using the processed conf
		Job secondJob = Job.getInstance(conf);

		firstInputPath = new Path(args[0]);
		secondInputPath = new Path(args[2]);
		// input directories
		// set the specific mapper for each input file
		MultipleInputs.addInputPath(secondJob, firstInputPath,
				TextInputFormat.class,
				SecondPhaseFirstFoafRetrievalMapper.class);
		MultipleInputs.addInputPath(secondJob, secondInputPath,
				TextInputFormat.class,
				SecondPhaseSecondFoafRetrievalMapper.class);
		// output directory
		FileOutputFormat.setOutputPath(secondJob, new Path(args[3]));

		// Define Map Output Classes (Key, Value)
		// We don't have to define this as it is the same as the Job Output.
		// But if it is not the same, you have to define it!
		secondJob.setMapOutputKeyClass(CompositeKeyWritable.class);
		secondJob.setMapOutputValueClass(Text.class);

		// set a custom partitioner that will take into account only the join
		// attribute
		secondJob.setPartitionerClass(FoafPartitioner.class);
		// set how the data to be ordered
		secondJob.setSortComparatorClass(FoafSortingComparator.class);
		// set a custom class that will be used for grouping within the reducer
		secondJob.setGroupingComparatorClass(FoafGroupingComparator.class);

		// Define Job Output Classes (Key, Value)
		secondJob.setOutputKeyClass(Text.class);
		secondJob.setOutputValueClass(Text.class);

		// set Reducer Class, we have already specify the two mappers
		secondJob.setReducerClass(SecondPhaseFoafRetrievalReducer.class);

		// Set the Number of Reduce Tasks
		secondJob.setNumReduceTasks(Integer.parseInt(args[4]));
		System.out.println("Number of reduce tasks:"
				+ Integer.parseInt(args[4]));

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		secondJob.setJarByClass(FoafRetrievalDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		secondJob.setJobName("FoafRetrieval Second Phase");

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return secondJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new FoafRetrievalDriver(), args);
		System.exit(exitCode);
	}
}
