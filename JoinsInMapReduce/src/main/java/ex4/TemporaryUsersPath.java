package ex4;

import java.util.ArrayList;

/**
 * The class that stores all useful information for a path such as <user1
 * foaf:knows ..... foaf:knows userN-1 foaf:knows userN distance>.
 * 
 * @author Polina Koleva
 *
 */
public class TemporaryUsersPath {

	private int distance;
	// all users which are already in the path
	private ArrayList<String> allUsersPerPath = new ArrayList<String>();
	private String endUser;
	private String startUser;
	private String path;

	public TemporaryUsersPath(String path) {
		this.path = path;
		// parse the statement <user1 foaf:knows user2 ... foaf:knows userN
		// distance>
		String[] pathParts = path.toString().trim().split(" ");
		int numberOfParts = pathParts.length;
		// the distance is the last element
		this.distance = Integer.parseInt(pathParts[numberOfParts - 1]);
		this.endUser = pathParts[numberOfParts - 2];
		this.startUser = pathParts[0];
		for (int i = 0; i < pathParts.length - 1; i += 2) {
			this.allUsersPerPath.add(pathParts[i]);
		}
	}

	public int getDistance() {
		return distance;
	}

	public ArrayList<String> getAllUsersPerPath() {
		return allUsersPerPath;
	}

	public String getEndUser() {
		return endUser;
	}

	public String getStartUser() {
		return startUser;
	}

	public String getPath() {
		return path;
	}
}
