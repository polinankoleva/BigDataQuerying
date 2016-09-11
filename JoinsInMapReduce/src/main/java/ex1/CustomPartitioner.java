package ex1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * A class which specify how are data will be divided into partitions and to
 * which reducer will be sent. Because we use {@link CompositeKeyWritable}, but
 * we want out data to be partitioned only by key attribute, the custom
 * partitioner is implemented. One reducer receives all data with the same key
 * attribute.
 * 
 * @author Polina Koleva
 *
 */
public class CustomPartitioner extends Partitioner<CompositeKeyWritable, Text> {

	@Override
	public int getPartition(CompositeKeyWritable key, Text value,
			int numberReduceTask) {
		return (key.getKey().hashCode() % numberReduceTask);
	}

}
