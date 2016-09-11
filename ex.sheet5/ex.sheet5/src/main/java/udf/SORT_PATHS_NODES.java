package udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * As an input parameter we have a list with already found paths <startUser
 * endUser>. Because the edges are in both directions, a path <user1 user5> is
 * the same as a path <user5 user1>. Because of this we sort the start and end
 * node of a path. In this way, the both paths will be reverted to the same one.
 * 
 * @author Polina Koleva
 *
 */
public class SORT_PATHS_NODES extends EvalFunc<Tuple> {

	TupleFactory mTupleFactory = TupleFactory.getInstance();
	BagFactory mBagFactory = BagFactory.getInstance();

	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return null;
		}
		Tuple tuple = (Tuple) input;
		String startPathUser = (String) tuple.get(0);
		String endPathUser = (String) tuple.get(1);
		String newStartPathUser = null;
		String newEndPathUser = null;
		// if we have a path is <user1 user5> the result path will be the same
		// But if we have a path <user5, user44>, the result is <user44, user5>
		if (startPathUser.compareTo(endPathUser) <= -1) {
			newStartPathUser = startPathUser;
			newEndPathUser = endPathUser;

		} else {
			newStartPathUser = endPathUser;
			newEndPathUser = startPathUser;
		}
		Tuple outputTuple = TupleFactory.getInstance().newTuple(2);
		outputTuple.set(0, newStartPathUser);
		outputTuple.set(1, newEndPathUser);
		return outputTuple;
	}

}
