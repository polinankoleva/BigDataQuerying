package ex1c;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The third mapper. It distributes all differences computed in the previous
 * map/reduce phase to one reducer.
 * 
 * @author Polina Koleva
 *
 */
public class PageRankDifferenceMapper extends
		Mapper<Object, Text, Text, FloatWritable> {

	// one static reducer key
	private final String DIFFERENCE_REDUCER_KEY = "pageDifferenceReducer";
	private Text pageDifferenceReducerKey = new Text(DIFFERENCE_REDUCER_KEY);

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// the statement has a format <userId pageRank (list of user's friends)
		// difference>
		// we need only the difference in this mapper
		String[] parsedStatement = value.toString().split("\t");
		float difference = Float.parseFloat(parsedStatement[3]);
		// sends the parsed difference to the only one reducer
		context.write(pageDifferenceReducerKey, new FloatWritable(difference));
	}
}
