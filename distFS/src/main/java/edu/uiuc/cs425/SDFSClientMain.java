package edu.uiuc.cs425;

import java.util.Scanner;

public class SDFSClientMain {
	
	public static void main( String[] args )
    {
		if( args.length !=1 )
		{
			System.out.println("Usage: java -cp ~/distFSClientFinal.jar edu.uiuc.cs425.App <xml_path>");
			System.exit(Commons.FAILURE);
		}
    	
    	// validation
		String sXML = args[0];
		
		ConfigAccessor m_oConfig = new ConfigAccessor();
		if( Commons.FAILURE == m_oConfig.Initialize(sXML))
		{
			System.out.println("Failed to Initialize XML");
			System.exit(1);
		}
		
		Logger m_oLogger		= new Logger();
		if( Commons.FAILURE == m_oLogger.Initialize("SDFSClient.log"))
		{
			System.out.println("Failed to Initialize logger object");
			System.exit(1);
		}
		
		SDFSClient client = new SDFSClient(m_oLogger, m_oConfig);
		Scanner 		m_oUserInput = new Scanner(System.in);
		
		
		while(true)
    	{
        	System.out.println("=========================");
        	System.out.println("Enter choice");
    		System.out.println("1. Add File");
    		System.out.println("2. Retrieve File");
    		System.out.println("3. Delete File");
    		System.out.println("4. List Files at Node");
    		System.out.println("5. List File locations");
    		System.out.println("6. Get full info");
    		System.out.println("Enter Input ");
    		String sInput = m_oUserInput.nextLine();
    		if( ! sInput.equals("1") && !sInput.equals("2") && !sInput.equals("3") &&
    				! sInput.equals("4") && !sInput.equals("5"))
    		{
    			System.out.println("Invalid input");
    			continue;
    		}
    		
    		if(sInput.equals("1"))
    		{
    			System.out.println("Local filepath");
    			String sLocal = m_oUserInput.nextLine();
    			System.out.println("SDFS name");
    			String sSDFS = m_oUserInput.nextLine();
    			client.AddFile(sLocal, sSDFS);
    		} else if(sInput.equals("2"))
    		{
    			System.out.println("SDFS name");
    			String sSDFS = m_oUserInput.nextLine();
    			System.out.println("Local filepath");
    			String sLocal = m_oUserInput.nextLine();
    			
    			client.GetFile(sSDFS, sLocal);
    		} else if(sInput.equals("3"))
    		{
    			System.out.println("SDFS name");
    			String sSDFS = m_oUserInput.nextLine();
    			
    			client.DeleteFile(sSDFS);
    		}  else if(sInput.equals("4"))
    		{
    			System.out.println("Enter NodeIP");
    			String sIP = m_oUserInput.nextLine();
    			client.FilesAt(sIP);
    		} else if(sInput.equals("5"))
    		{
    			System.out.println("SDFS name");
    			String sSDFS = m_oUserInput.nextLine();
    			client.FileLoc(sSDFS);
    		} else if(sInput.equals("6"))
    		{
    			client.GetFullInfo();;
    		}
    		try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
		
		
		
    }
}
