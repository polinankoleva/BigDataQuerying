package ex4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. The input file used by this mapper is the file
 * produced from the previous job. This file has statements with format <user1
 * foaf:knows user2 ... foaf:knows userN distance>. We set the final user of the
 * path(userN) as a key out. Moreover, the reducer for this key will receive all
 * paths with last user <userN>(such like the given) and all triples <userN
 * foaf:knows userK>. We can join them by <userN> and continue adding edges to
 * our path until reaching the given end user. The used reducer is
 * {@link SecondPhaseFoafPathFinderReducer}. The second used mapper is
 * {@link SecondPhaseFirstFoafPathFinderMapper}.
 * 
 * 
 * @author Polina Koleva
 *
 */
public class SecondPhaseSecondFoafPathFinderMapper extends
		Mapper<Object, Text, CompositeKeyWritable, Text> {

	// source index for the second data set
	private static int sourceIndex = 0;
	// the composite key used to send data to the reducer
	private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
	// a variable which will store the value send to the reducer
	private Text statementText = new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// parse the statement <user1 foaf:knows user2 ... foaf:knows userN
		// distance>
		String[] statementParts = value.toString().trim().split(" ");
		// set as a key last user from the path - in this case to the reducer
		// will be sent all paths with
		// last user <userN> and all triples <userN foaf:knows userK>. We can
		// join them by <userN> and continue
		// adding edges to our path until reaching the given end user
		String lastUser = statementParts[statementParts.length - 2];
		compositeKey.setJoinKey(lastUser);
		compositeKey.setSourceIndex(sourceIndex);
		statementText.set(value.toString());
		context.write(compositeKey, statementText);
	}
}
