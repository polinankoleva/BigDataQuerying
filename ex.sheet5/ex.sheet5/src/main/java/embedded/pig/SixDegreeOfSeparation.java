package embedded.pig;

import java.io.IOException;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * Main execution of embedded pig program. It finds maximum length of a path in
 * a graph.
 * 
 * @author Polina Koleva
 *
 */
public class SixDegreeOfSeparation {

	public static void main(String[] args) throws ExecException, IOException {
		/*
		 * Validate that five arguments which were passed from the command
		 * line.
		 */
		if (args.length != 5) {
			System.out
					.printf("Usage: SixDegreeOfSeparation <pig mode> <rdf storage jar path> <udf jar path> <input path> <output path>\n");
			System.exit(-1);
		}
		String pigMode = args[0];
		String rdfStoragePath = args[1];
		String udfPath = args[2];
		String inputPath = args[3];
		String outputPath = args[4];
		PigServer pigServer = new PigServer(pigMode);
		try {
			pigServer.registerJar(rdfStoragePath);
			pigServer.registerJar(udfPath);
			findPaths(pigServer, inputPath, outputPath, "foaf:knows");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void findPaths(PigServer pigServer, String inputFile,
			String outputFile, String predicate) throws IOException {
		// loads data
		pigServer.registerQuery("indata = LOAD'" + inputFile
				+ "' USING RDFStorage() AS (s,p,o);");
		// filters it by predicate "foaf:knows"
		pigServer.registerQuery("filterData = FILTER indata BY p == '"
				+ predicate + "' ;");
		// stores only subject object, removes the predicate foaf:knows
		// all paths with length 1
		pigServer
				.registerQuery("initialPaths = FOREACH filterData GENERATE (chararray) $0 as user1:chararray, (chararray) $2 as user2:chararray ;");
		pigServer
				.registerQuery("initialBidirectionalPathsWithDuplicates = FOREACH initialPaths GENERATE "
						+ "udf.SORT_PATHS_NODES(user1, user2) ;");
		// removes duplicate paths ; no risk of <user1 user1> at this point
		pigServer
				.registerQuery("foundBidirectionalPaths = DISTINCT initialBidirectionalPathsWithDuplicates ;");

		// gets count of found paths
		pigServer
				.registerQuery("foundPathsCount = FOREACH (GROUP foundBidirectionalPaths ALL) GENERATE COUNT(foundBidirectionalPaths);");
		long initialCount = (Long) pigServer.openIterator("foundPathsCount")
				.next().get(0);
		// variable where the maximum path length is stored
		int maxPathLenght = 1;
		while (true) {
			// tries to extend already found paths by joining them with a graph
			// again
			// tries to extend a path by adding an edge to its left side
			pigServer
					.registerQuery("leftPathExtensionJoin = JOIN foundBidirectionalPaths BY $0.$0, initialPaths BY user1 ;");
			pigServer
					.registerQuery("leftPathExtension =  FOREACH leftPathExtensionJoin GENERATE $0.$1, $2 ;");
			// tries to extend a path by adding an edge to its right side
			pigServer
					.registerQuery("rightPathExtensionJoin = JOIN foundBidirectionalPaths BY $0.$1, initialPaths BY user1 ;");
			pigServer
					.registerQuery("rightPathExtension =  FOREACH rightPathExtensionJoin GENERATE $0.$0, $2 ;");
			// unions already found paths from the join
			pigServer
					.registerQuery("pathExtensions = UNION leftPathExtension, rightPathExtension ;");

			// sorts egdes of each path. Finally instead of two different
			// representation of a path <user1 user5> and <user5 user1>,
			// we have only one <user1 user5> twice
			pigServer
					.registerQuery("pathExtensionsBidirectionalPathsWithDuplicates = FOREACH pathExtensions GENERATE "
							+ "udf.SORT_PATHS_NODES($0, $1) ;");

			// union the new found paths by the join and those which were found
			// before the join
			pigServer
					.registerQuery("allFoundPaths = UNION foundBidirectionalPaths, pathExtensionsBidirectionalPathsWithDuplicates ; ");
			// removes duplicate paths, because between two nodes there can be
			// more than one path
			pigServer
					.registerQuery("allFoundPathsWithSameElements = DISTINCT allFoundPaths ;");
			// removes all paths between the same node, for example <user1
			// user1>
			pigServer
					.registerQuery("foundBidirectionalPaths = FILTER allFoundPathsWithSameElements BY $0.$0 != $0.$1 ;");

			// get count of all found paths after this iteration
			pigServer
					.registerQuery("foundPathsCount = FOREACH (GROUP foundBidirectionalPaths ALL) GENERATE COUNT(foundBidirectionalPaths);");
			long allFoundPaths = (Long) pigServer
					.openIterator("foundPathsCount").next().get(0);
			// if there are new found paths in this iteration - stop searching
			// for paths
			if (allFoundPaths == initialCount) {
				break;
				// if we have added new paths found in this iteration,
				// increases the maximum length path because by each iteration
				// we find paths
				// with length +1 more than the previous iteration
			} else {
				maxPathLenght++;
				initialCount = allFoundPaths;
			}
		}
		// stores all found paths
		pigServer.store("foundBidirectionalPaths", outputFile);
		System.out.println("Max path lenght:" + maxPathLenght);
	}
}
