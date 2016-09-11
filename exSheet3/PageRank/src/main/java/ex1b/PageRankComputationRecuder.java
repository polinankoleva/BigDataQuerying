package ex1b;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The second reducer. It computes the final page rank of an user(node) after
 * the current iteration. It just sums over all received portions of page rank
 * by incoming edges of the node. Besides, one of the incoming values is list
 * with an user's friends which is just needed for the next iteration.
 * 
 * @author Polina Koleva
 *
 */
public class PageRankComputationRecuder extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		String friends = null;
		float finalPageRank = 0f;
		while (iterator.hasNext()) {
			Text next = iterator.next();
			try {
				// sum over all incoming parts of page rank
				float incomingPageRank = Float.parseFloat(next.toString());
				finalPageRank += incomingPageRank;
			} catch (NumberFormatException e) {
				// the incoming value is just a list with user's friends
				friends = next.toString();
			}
		}
		// write <userId updatedPageRank (user's friends)>
		context.write(key, new Text(finalPageRank + "\t" + friends));
	}
}
