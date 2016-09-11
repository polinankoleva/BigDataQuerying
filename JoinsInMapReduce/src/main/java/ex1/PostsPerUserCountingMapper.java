package ex1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Second Mapper. It processes all tuples <userId 1> and add as a key out the
 * userId. One reducer per user.
 * 
 * @author Polina Koleva
 *
 */
public class PostsPerUserCountingMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	private Text userIdKey = new Text();
	private IntWritable numberOfPostsValue = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// process tuples of format <userId #posts> #posts - 1 for our case
		StringTokenizer rdfStatement = new StringTokenizer(value.toString());
		String userId = rdfStatement.nextToken();
		int numberOfPosts = Integer.parseInt(rdfStatement.nextToken());
		// a reducer per user where all numbersOfPosts will be sent
		userIdKey.set(userId);
		numberOfPostsValue.set(numberOfPosts);
		context.write(userIdKey, numberOfPostsValue);
	}
}
