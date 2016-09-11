package ex4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer class that reduces a set of intermediate values which share a key
 * to a smaller set of values. It receives an input formatted as <username <2 3
 * 5 .. 6>>(Usually, if the combiner is not specified, the input will be
 * <username <1 1 1 ... 1>>). The key which comes in is the user name for the
 * user who's number of incoming edges with predicate "foaf:knows" is counted.
 * The values coming in are the numbers of these edges computed by the mapper.
 * We collect them and sum them. Finally, we receive the "total popularity" of
 * the user (It is equal to the total number of ingoing edges with predicate
 * "foaf:knows" per user). and can select which users will be included into the
 * final set. If the popularity of an user is more or equal that the popularity
 * threshold, the user is included, otherwise it isn't/
 * 
 * @author Polina Koleva
 *
 */
public class UsersSelectionByPopularityReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	// a variable that is used for storing of total number of incoming edges
	// with predicate "foaf:knows" per user
	private IntWritable totalPopularityPerUser = new IntWritable();
	// variable that stored a popularity threshold by which the selection of
	// user will be made
	private int popularityThreshold;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// parse the popularity threshold that is passed as a input parameter
		popularityThreshold = context.getConfiguration().getInt(
				UsersSelectionByPopularityDriver.popularityThreshold, 1);
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int popularityCount = 0;
		Iterator<IntWritable> it = values.iterator();
		// sum over all received #incoming edges with predicate "foaf:knows" per
		// user
		while (it.hasNext()) {
			int next = it.next().get();
			popularityCount += next;
		}
		// if the popularity of an user is more or equal to specified as an
		// input popularity threshold
		// include the user in the result set
		if (popularityCount >= popularityThreshold) {
			totalPopularityPerUser.set(popularityCount);
			context.write(key, totalPopularityPerUser);
		}
	}
}
