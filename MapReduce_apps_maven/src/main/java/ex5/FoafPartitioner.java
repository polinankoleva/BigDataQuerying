package ex5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A class which specify how are data will be partioned and to which reducer
 * will be sent. Because we use {@link CompositeKeyWritable}, but we want out
 * data to be partitioned only by join attribute, the custom partitioner is
 * implemented. One reducer receives all data with the same join attribute.
 * 
 * @author Polina Koleva
 *
 */
public class FoafPartitioner extends Partitioner<CompositeKeyWritable, Text> {

	@Override
	public int getPartition(CompositeKeyWritable key, Text value,
			int numberReduceTask) {
		// Partitions on joinKey (username)
		return (key.getJoinKey().hashCode() % numberReduceTask);
	}

}
