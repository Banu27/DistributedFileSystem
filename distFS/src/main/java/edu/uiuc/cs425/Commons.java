package edu.uiuc.cs425;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public final class Commons {
	public static final int SUCCESS = 0;
	public static final int FAILURE = 1;
	public static final int SERVICE_PORT = 9090;
	public static final int NETWORK_THREAD_COUNT = 4;
	public static final int WORKER_THREAD_COUNT = 8;
	public static final int MASTER = 0;
	public static final int NUMBER_OF_VMS = 6;
	public static final String [] VM_NAMES = {"fa15-cs425-g01-01.cs.illinois.edu", 
			"fa15-cs425-g01-02.cs.illinois.edu",
			"fa15-cs425-g01-03.cs.illinois.edu",
			//"fa15-cs425-g01-04.cs.illinois.edu",
			"fa15-cs425-g01-05.cs.illinois.edu",
			"fa15-cs425-g01-06.cs.illinois.edu",
			"fa15-cs425-g01-07.cs.illinois.edu" };
	public static final String username = "muthkmr2";
	public static int aliveNumber = NUMBER_OF_VMS;
	public static final String NODE_INTROCUDER = "introducer";
	public static final String NODE_PARTICIPANT = "participant";
	public static final String SDFS_LOC = "$HOME/SDFS/";
	
	//Floyd's - Random 
	static Set<Integer> RandomK(int m, int max, long seed)
	{
		Random rnd = new Random(seed);
		int n = max;
		HashSet<Integer> res = new HashSet<Integer>(m);
	    for(int i = n - m; i < n; i++){
	        int item = rnd.nextInt(i + 1);
	        if (res.contains(item))
	            res.add(i);
	        else
	            res.add(item); 
	    }
	    return res;
	}
	
	
	public static void SystemCommand(String[] command) {
		
		String s = null;
		
		try {
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			System.out.println("Done with call "+ command);
				
			// read the output from the command
			//System.out.println("Here is the standard output of the command:\n");
			while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
			}

			// read any errors from the attempted command
			//System.out.println("Here is the standard error of the command (if any):\n");
			while ((s = stdError.readLine()) != null) {
				System.out.println(s);
	        }
	             
		
		} catch (IOException e) {
            System.out.println("exception happened - here's what I know: ");
            e.printStackTrace();
            System.exit(-1);
		}
	}
	
	
}
