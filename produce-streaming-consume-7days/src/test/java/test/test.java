package test;

public class test {
	public static void main(String args[]) {
		String recordValue = "NYL00620_7A_1,NYL00620,20170704,NY/NNJ,13.15.0,Northeast,NYC East,n50-10MHz,5780,309136741.0,10344076929.0,8270454393.0,1930562519.04,1491367.394,2256932773.89,1583052.501,";
		int comma = 0, walker = 0, runner = -1;
		String region = null, market = null;
		while (++runner < recordValue.length()) {
			if (recordValue.charAt(runner) != ',')
				continue;
			comma++;
			if (comma == 5)
				walker = runner + 1;
			else if (comma == 6) {
				region = recordValue.substring(walker, runner);
				walker = runner + 1;
			} else if (comma == 7)
				market = recordValue.substring(walker, runner);
			else if (comma == 12)
				break;
		}
		String newKey = "Region:" + region + "_Market:" + market;
		String newRecord = recordValue.substring(runner + 1, recordValue.length() - 1);
		System.exit(0);
	}
}
