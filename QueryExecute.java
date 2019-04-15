
public class QueryExecute {
	public static void main(String args[]) {
		if(args.length < 1) {
			System.out.println("Enter a file containing queries aas an argument");
			return;
		}

		System.out.println("file: "+args[0]);
	}

}
