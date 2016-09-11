package ex2b;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class that reduces a set of intermediate values which share a key
 * to a smaller set of values. It receives an input formatted as <userM <1 2 3
 * ... 6>>(The values here are not 1 because we have a combiner class set). The
 * key which comes in is the user name of the user for whom the number of likes
 * will be counted. The values coming in are the numbers of likes this user did.
 * We collect them and sum them. Finally, we set as a key out the user name of
 * the user and as a value out the final sum of all likes for the user.
 * 
 * @author Polina Koleva
 *
 */
public class LikeCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	// variable for storing total number of likes per user
	private IntWritable totalLikeCount = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int likeCount = 0;
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			// sum over all coming numbers of likes
			int next = it.next().get();
			likeCount += next;
		}
		totalLikeCount.set(likeCount);
		// set as a result the user and its total number of likes
		context.write(key, totalLikeCount);
	}
}
