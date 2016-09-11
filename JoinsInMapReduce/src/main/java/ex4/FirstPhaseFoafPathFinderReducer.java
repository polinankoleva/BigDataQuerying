package ex4;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The reducer class that reduces a set of intermediate values which share a key
 * to a smaller set of values. It is used for reduce-side join. The mapper class
 * {@link FirstPhaseFoafPathFinderMapper} processes data in a way that it is
 * ready for the join. The reducer is responsible to join the two data sets and
 * to produce the result list containing records <fromUser foaf:knows userN
 * foaf:knows userM>. For example, let's have from the first data set <fromUser
 * foaf:knows userN> and from the second data set <<userN foaf:knows userM>,
 * <userN foaf:knows UserK>>. Firstly, it will be checked if the userN is the
 * final end user from the search path. If it is, the data will be stored and
 * the program will terminate. Otherwise, it will form statements like <fromUser
 * foaf:knows userN foaf:knows UserM> or <fromUser foaf:knows userN foaf:knows
 * UserK>. If one of the end users(userM/userK) at these paths is out final
 * searched end user, we find a path, store it to the final paths file.
 * Otherwise, store all created statements into temporary paths file.
 * 
 * 
 * @author Polina Koleva
 *
 */
public class FirstPhaseFoafPathFinderReducer extends
		Reducer<CompositeKeyWritable, Text, Text, Text> {

	private Text valueOut = new Text();
	// a variable where the reducer key will be stored
	private Text reducerKey = new Text();
	// multiple output files - one is used for temporary paths
	// another one for final paths <fromUser .... toUser> if it is found
	MultipleOutputs<Text, Text> mos;

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs(context);
	}

	@Override
	public void reduce(CompositeKeyWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		// parse the end user of our searched path
		String toUser = context.getConfiguration().get("toUser");
		String fromUser = null;
		// we know that the entries for "the first join" data set (sourceIndex =
		// 0) will come first
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			Text next = it.next();
			// parse the statement <subject predicate object>
			StringTokenizer rdfStatement = new StringTokenizer(next.toString());
			String subject = rdfStatement.nextToken();
			String predicate = rdfStatement.nextToken();
			String object = rdfStatement.nextToken();
			if (key.getSourceIndex() == 0) {
				// only one or zero triples are coming from data set 0
				// <fromPathUser foaf:knows userN>
				fromUser = subject;
				// if the userN is the searched endPathUser
				if (object.equals(toUser)) {
					// a path with distance 1 is found
					// use an empty key
					reducerKey.set("");
					valueOut.set(next.toString() + " " + 1);
					// add the found path in the file named "finalPaths"
					mos.write("finalPaths", reducerKey, valueOut);
					return;
				}
				// receives data from the second data set <userN foaf:knows
				// userM>
				// and check if there is data from the first data set
				// because we filter by "fromPathUser", it is possible to not
				// receive such data
			} else if (key.getSourceIndex() == 1 && fromUser != null) {
				// check if we reach our endPathUser
				if (!object.equals(fromUser)) {
					// we find a path with distance two
					if (object.equals(toUser)) {
						valueOut.set(fromUser + " " + predicate + " "
								+ next.toString() + " " + 2);
						// store the final path to the file named "finalPaths"
						mos.write("finalPaths", reducerKey, valueOut);
						return;
					} else {
						// if we have to continue searching - write
						// already found paths of format <fromUser foaf:knows
						// userN foaf:knows userM>
						// to the file named "temporaryPaths"
						valueOut.set(fromUser + " " + predicate + " "
								+ next.toString() + " " + 2);
						mos.write("temporaryPaths", reducerKey, valueOut);
					}
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}
}
