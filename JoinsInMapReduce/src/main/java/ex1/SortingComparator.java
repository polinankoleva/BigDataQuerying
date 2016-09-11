package ex1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * A class used for sorting of the data. First, two records will be sorted by
 * their key attribute and after that by their index. For example, if we have
 * two records "record" from the first data set (source index = 0) and the same
 * record from the second data set (source index = 1). Those from the first will
 * be before those from the second.
 * 
 * @author Polina Koleva
 *
 */
public class SortingComparator extends WritableComparator {

	protected SortingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		// Sort on all attributes of composite key
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getKey().compareTo(key2.getKey());
		if (cmpResult == 0) // same key
		{
			return Double.compare(key1.getSourceIndex(), key2.getSourceIndex());
		}
		return cmpResult;
	}
}
