package ex4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The reducer class that reduces a set of intermediate values which share a
 * key. This reducer is used for reduce-side join. The mapper classes
 * {@link SecondPhaseFirstFoafPathFinderMapper} and
 * {@link SecondPhaseSecondFoafPathFinderMapper}. They process data in a way
 * that it is ready for the join. The reducer is responsible to join the two
 * data sets and to produce the result list containing records <fromUser
 * foaf:knows ..... foaf:knows userM distance> or final paths <fromUser
 * foaf:knows....foaf:knows toUser>. For example, let's have from the first data
 * set <fromUser foaf:knows ... foaf:knows userN> and from the second data set,
 * we receive <userN foaf:knows userK>. Firstly, will be checked is <userK> is
 * the final path user we are searching for. If true, a new path <fromUser
 * foaf:knows ..... foaf:knows userM foaf:knows userK distance+1> will be built
 * and store to the file with final paths. The program could stop processing.
 * Otherwise, a new path <fromUser foaf:knows ..... foaf:knows userM foaf:knows
 * userK distance+1> is built and store to the file with temporary paths. On the
 * next map/reduce phase, building of possible paths will continue from these
 * temporary paths and will again search for new possible edges to add.
 * 
 * @author Polina Koleva
 *
 */
public class SecondPhaseFoafPathFinderReducer extends
		Reducer<CompositeKeyWritable, Text, Text, Text> {

	private Text valueOut = new Text();
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
		// parse the searched final user
		String toUser = context.getConfiguration().get("toUser");
		// array where we store all already found temporary paths which we have
		// to continue with edges
		ArrayList<TemporaryUsersPath> temporaryUsersPaths = new ArrayList<TemporaryUsersPath>();
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			Text next = it.next();
			// firstly all temporary paths <fromUser foaf:knows ..... foaf:knows
			// userN> are received
			if (key.getSourceIndex() == 0) {
				// just parse and store them
				TemporaryUsersPath path = new TemporaryUsersPath(
						next.toString());
				temporaryUsersPaths.add(path);
				// secondly, all triples <userN foaf:knows userM> have to be
				// processed
				// there is a possibility a path such as <fromUser foaf:knows
				// ..... foaf:knows userN> to not
				// exist(end user - userN), so if this happens - no needed
				// information in this reducer
				// no more processing
			} else if (key.getSourceIndex() == 1
					&& !temporaryUsersPaths.isEmpty()) {
				// parse rdf statement into subject, predicate , object
				StringTokenizer rdfStatement = new StringTokenizer(
						next.toString());
				// a triple <userN foaf:knows userM> with which we want to
				// continue already existing path
				String subject = rdfStatement.nextToken();
				String predicate = rdfStatement.nextToken();
				String object = rdfStatement.nextToken();
				// if the object is equal to our searched final user
				// add the user(object) to all temporary paths, increase their
				// distance by 1
				// and store them to the file with final paths named
				// "finalPaths"
				if (object.equals(toUser)) {
					for (int i = 0; i < temporaryUsersPaths.size(); i++) {
						TemporaryUsersPath path = temporaryUsersPaths.get(i);
						String newPath = addUserAndIncreaseDistance(path,
								object);
						// we find a final path
						// use an empty key
						reducerKey.set("");
						valueOut.set(newPath);
						mos.write("finalPaths", reducerKey, valueOut);
						return;
					}
				} else {
					// the parsed triple <userN foaf:knows userM> does not
					// end with the searched final user
					// so continue building our temporary paths
					for (int i = 0; i < temporaryUsersPaths.size(); i++) {
						TemporaryUsersPath path = temporaryUsersPaths.get(i);
						// we don't want to have cycles, so we check if user
						// that will be added
						// to the path already is part of it
						if (!path.getAllUsersPerPath().contains(object)) {
							// increases the distance and adds the new parsed
							// user
							// store the new built path into a file
							// "temporaryPaths"
							// where we will continue building longer paths
							String newPath = addUserAndIncreaseDistance(path,
									object);
							reducerKey.set("");
							valueOut.set(newPath);
							mos.write("temporaryPaths", reducerKey, valueOut);
						}
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

	// adds a new user to a path and increase its distance
	// finally just format it as it is needed
	public String addUserAndIncreaseDistance(TemporaryUsersPath path,
			String user) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < path.getAllUsersPerPath().size(); i++) {
			sb.append(path.getAllUsersPerPath().get(i) + " " + "foaf:knows"
					+ " ");
		}
		sb.append(user + " ");
		sb.append(path.getDistance() + 1);
		return sb.toString();
	}
}
