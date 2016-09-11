package ex3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A mapper class that represents individual task and transforms input records
 * into a intermediate records. It receives a file with statements formatted as
 * <username #likes>. It goes over them and just set as a key the same for all
 * of them. Therefore, all statements will reach one reducer where the number of
 * likes can be calculated by sum over all #likes in all statements. As a value,
 * as we already implicitly mentioned, the #likes per user will be set. The
 * mapper key is text, but the value are intWritable. The output of the mapper
 * will be <defaultKey #likes> for one parsed statement.
 * 
 * @author Polina Koleva
 *
 */
public class AverageLikeCountMapper extends
		Mapper<Object, Text, Text, IntWritable> {

	// the reduce key that will be used for all parsed statements
	private final static Text onlyReducerKey = new Text("oneReducer");
	// variable where the total #likes per user is stored
	private IntWritable likesCount = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer tuple = new StringTokenizer(value.toString());
		String username = tuple.nextToken();
		// gets #likes per user
		int likeCountNumber = Integer.parseInt(tuple.nextToken());
		likesCount.set(likeCountNumber);
		// sets the same reduce key for all parsed statements
		context.write(onlyReducerKey, likesCount);

	}
}
