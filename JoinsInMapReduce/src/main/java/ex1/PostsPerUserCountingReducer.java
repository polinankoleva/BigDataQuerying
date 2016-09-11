package ex1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The second reducer. A reducer per user. it sums over all #posts created by an
 * user and results into a final number of created posts by an user.
 * 
 * @author Polina Koleva
 *
 */
public class PostsPerUserCountingReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	// variable for storing total number of posts created by an user
	private IntWritable totalUserPosts = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int postsCount = 0;
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			// sum over all coming numbers of posts
			int next = it.next().get();
			postsCount += next;
		}
		totalUserPosts.set(postsCount);
		// set as a result the user and its total number of posts
		context.write(key, totalUserPosts);
	}
}
