package ex1c;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The third reducer. It goes over all differences and sums them. If the
 * received sum is small enough, we assume it is time to terminate the algorithm
 * (no more needed iterations.)
 * 
 * @author Polina Koleva
 *
 */
public class PageRankDifferenceReducer extends
		Reducer<Text, FloatWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<FloatWritable> iterator = values.iterator();
		float sum = 0f;
		while (iterator.hasNext()) {
			// goes over all sent differences and sums them
			sum += iterator.next().get();
		}
		// if the overall sum is smaller enough, we assume that the page ranks
		// will soon converge
		// so we can terminate the program
		if (sum < 0.002) {
			// set a termination condition to 1(TRUE)
			context.getCounter(CustomCounters.TERMINATE_EXECUTION).setValue(1);
		}
	}
}
