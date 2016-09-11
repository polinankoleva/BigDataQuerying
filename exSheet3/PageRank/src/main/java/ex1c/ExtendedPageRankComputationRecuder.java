package ex1c;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The second reducer. It computes the final page rank of an user(node) after
 * the current iteration. It just sums over all received portions of page rank
 * by incoming edges of the node. Besides, one of the incoming values is list
 * with an user's friends which is just needed for the next iteration. Moreover,
 * the initial for the current iteration page rank of the user is also sent to
 * the reducer. By it, we can compute the difference between the initial and the
 * updated page rank. If they don't differ too much, we can assume that the
 * iterations algorithms run are enough and terminate the program. For more
 * information about that see - {@link PageRankDifferenceMapper} and
 * {@link PageRankDifferenceReducer}.
 * 
 * @author Polina Koleva
 *
 */
public class ExtendedPageRankComputationRecuder extends
		Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		String friends = null;
		float finalPageRank = 0f;
		float pageRank = 0f;
		float initialPageRank = 0f;
		while (iterator.hasNext()) {
			String next = iterator.next().toString();
			try {
				// sum over all incoming parts of page rank
				pageRank = Float.parseFloat(next);
				finalPageRank += pageRank;
			} catch (NumberFormatException e) {
				// the incoming value is the initial page rank for the user
				if (next.startsWith("initialPageRank")) {
					initialPageRank = Float.parseFloat(next.split(":")[1]);
				} else {
					// the incoming value is just a list with user's friends
					friends = next.toString();
				}
			}
		}
		// add the difference between the initial and updated page rank for this
		// iteration to
		// the output. It will be used by the next map/reduce phase as a means
		// for deciding
		// if the already done iterations are enough
		float difference = Math.abs(initialPageRank - finalPageRank);
		context.write(key, new Text(finalPageRank + "\t" + friends + "\t"
				+ difference));
	}
}
