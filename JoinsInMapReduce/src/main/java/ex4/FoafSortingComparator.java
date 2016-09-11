package ex4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A class used for sorting of the data. First, two records will be sorted by
 * their join attribute and after that by their index. For example, if we have
 * two records "user1 foaf:knows user2" from the first data set (source index =
 * 0) and the same record from the second data set (source index = 1). Those
 * from the first will be before those from the second.
 * 
 * @author Polina Koleva
 *
 */
public class FoafSortingComparator extends WritableComparator {

	protected FoafSortingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// Sort on all attributes of composite key
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getJoinKey().compareTo(key2.getJoinKey());
		if (cmpResult == 0)// same joinKey
		{
			return Double.compare(key1.getSourceIndex(), key2.getSourceIndex());
		}
		return cmpResult;
	}
}
