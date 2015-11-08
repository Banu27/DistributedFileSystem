package edu.uiuc.cs425;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Scanner;

import org.apache.thrift.TException;

public class Controller {
	
	private ConfigAccessor  m_oConfig;
	private CommServer      m_oCommServ;
	private Membership      m_oMember;
	private Introducer      m_oIntroducer;
	private Heartbeat       m_oHeartbeat;
	private String 			m_sNodeType;
	private Thread          m_HBThread;
	private Thread 			m_FailDetThread;
	private Thread          m_FRHBThread;
	private Logger 			m_oLogger;
	private Scanner 		m_oUserInput;
	private String 			introIP;
	private String 			hostIP; //What is this? - Banu
	private Election		m_oElection;
	private SDFSMaster		m_oSDFSMaster;
	private NodeFileMgr     m_oNodeMgr;
	
	public Controller()
	{
		m_oConfig 			= new ConfigAccessor();
		m_oCommServ			= new CommServer();
		m_oMember       	= new Membership();
		m_oIntroducer   	= new Introducer();
		m_oHeartbeat    	= new Heartbeat();
		m_oLogger			= new Logger();
		m_oUserInput    	= new Scanner(System.in);
		m_oElection 		= new Election();
		m_oSDFSMaster		= new SDFSMaster();
		m_oNodeMgr      	= new NodeFileMgr();
	}
	
	public int Initialize(String sXML)
	{
		
		if( Commons.FAILURE == m_oConfig.Initialize(sXML))
		{
			System.out.println("Failed to Initialize XML");
			return Commons.FAILURE;
		}
		
		if( Commons.FAILURE == m_oLogger.Initialize(m_oConfig.LogPath()))
		{
			System.out.println("Failed to Initialize logger object");
			return Commons.FAILURE;
		}
		
		introIP = m_oConfig.IntroducerIP();
		hostIP = null;
		try {
			hostIP  = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}
		
		m_oLogger.Info("IP: " + hostIP);
		m_oLogger.Info("Intro IP: " + introIP);
		
		
		if(introIP.equals(hostIP))
			m_sNodeType = Commons.NODE_INTROCUDER;
		else
			m_sNodeType = Commons.NODE_PARTICIPANT;
		
		
		m_oLogger.Info("Nodetype: " + m_sNodeType);
		
		//Membership object of class initializing here
		if( Commons.FAILURE == m_oMember.Initialize(m_oConfig, m_oLogger, introIP, m_oElection) )
		{
			m_oLogger.Error("Failed to initialize the membership object");
			return Commons.FAILURE;
		}
		// nodemanager init
		if ( Commons.FAILURE == m_oNodeMgr.Initialize(m_oLogger, m_oConfig, m_oMember.GetIP(m_oMember.UniqueId()),
				m_oElection,m_oMember))
		{
			m_oLogger.Error("Failed to initialize the Node Manager");
			return Commons.FAILURE;
		}
		//master init
		if(  Commons.FAILURE == m_oSDFSMaster.Initialize(m_oMember, m_oLogger, m_oConfig.CmdPort(), 
				m_oElection, m_oNodeMgr, m_oConfig))
		{
			m_oLogger.Error("Failed to initialize the Node Manager");
			return Commons.FAILURE;
		}
		
		//Initializing Election object- after master init
		m_oElection.Initialize(m_oMember,m_oLogger,m_oConfig.CmdPort(),m_oSDFSMaster);
		
		//CommServer object initializing here
		//ToDo - Use CommServer here for all and not just intoducer
		
		if ( Commons.FAILURE == m_oIntroducer.Initialize(m_oMember, m_oLogger, m_oElection,m_oConfig) )
		{
			m_oLogger.Error("Failed to initialize the communication server");
			return Commons.FAILURE;
			
		}
		
		if(Commons.FAILURE == m_oCommServ.Initialize(m_oMember, m_oIntroducer, m_oElection, 
				m_oSDFSMaster, m_oNodeMgr,m_oLogger,m_oConfig))
		{
			m_oLogger.Error("Failed to initialize the communication server");
			return Commons.FAILURE;
		}		
		
		//Intializing Heartbeat object
		if( Commons.FAILURE == m_oHeartbeat.Initialize(m_oMember, m_oConfig, m_oLogger))
		{
			m_oLogger.Error("Failed to initialize the heartbeat sender");
			return Commons.FAILURE;
		}
		return Commons.SUCCESS;
	}
	

	//Starting the thrift and UDP servers here
	public void StartAllServices()
	{
		m_oCommServ.StartCmdService(m_oConfig.CmdPort()); //Giving introducer port here
		// bring up the heartbeat receiver
		m_oCommServ.StartHeartBeatRecvr();
		//breing up the file report recvr
		m_oCommServ.StartFileReportRecvr();
	}
	
	public void StartMemberlistHB()
	{
		// start heart beating thread
		m_HBThread = new Thread(m_oHeartbeat);
		m_HBThread.start();
	}
	
	public void StartFRHB()
	{
		//start filereport sender thread
		m_FRHBThread = new Thread(m_oNodeMgr);
		m_FRHBThread.start();
	}
	
	public void StartFailureDetection()
	{
		//start failure detection thread
		m_FailDetThread = new Thread(m_oMember);
		m_FailDetThread.start();
	}
	
	
	public void StartReplicationMgr()
	{
		// check if the current node is the leader. If yes start replication check thread
		if( hostIP.equals(m_oElection.GetLeaderIP()))
		{
			m_oSDFSMaster.StartReplicationMgr();
		}
	}
	
	public void LeaveList()
	{
		// stop heartbeating 
		m_oHeartbeat.StopHB();
		
		// call to memebership to set node entry to leave status
		m_oMember.TimeToLeave();
		// do final HB
		try {
			m_oHeartbeat.DoHB();
		}  catch (Exception e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		}
		System.exit(0);
	}
	
	
	public int IntroduceSelf()
	{	
		//Introducer always comes up with serialNumber 0
		if(m_sNodeType.equals(Commons.NODE_INTROCUDER))
		{
			m_oLogger.Info(new String("Adding self as introducer and leader with Sno 0"));
			//If recover from checkpoint, set new leader in election. Don't reset serial number
			m_oMember.AddSelf(0);
			m_oElection.SetSerialNumber(0);
			m_oElection.SetLeader(m_oMember.UniqueId());
			RecoverFromCheckPoint();
		}
		else if(m_sNodeType.equals(Commons.NODE_PARTICIPANT))
		{
			CommandIfaceProxy proxy = new CommandIfaceProxy();
			int counter = 0;

			// continuous pinging for introducer to connect
			while(Commons.FAILURE == proxy.Initialize(m_oConfig.IntroducerIP(), m_oConfig.CmdPort(), m_oLogger))
			{
				if( counter++ > 100) 
				{
					m_oLogger.Error("Failed to connect to Introducer. Exiting after 100 tries");
					return Commons.FAILURE;
				}
				
				// sleep 5 secs before next retry
				m_oLogger.Warning("Failed to connect to Introducer. Trying again in 5 secs");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					return Commons.FAILURE;
				}
			}
			// checkpointing will change this part of the logic
			//Added call to JoinGroup for getting serialNumbers
			
			try {
				int serialNumber = proxy.JoinGroup();
				m_oLogger.Info(new String("Received serial number from introdcr : " + String.valueOf(serialNumber)));
				m_oMember.AddSelf(serialNumber);
				m_oElection.SetSerialNumber(serialNumber);
			} catch (TException e2) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e2));
			}
			int leaderCounter = 0;
			try {
				while(!proxy.IsLeaderAlive())
				{
					if( leaderCounter++ > 10) 
					{
						m_oLogger.Error("Failed to receive leader. Exiting after 10 tries");
						return Commons.FAILURE;
					}
					
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
					}
					m_oLogger.Warning(new String("Leader not alive. Trying in 5 secs"));			
				}
				m_oElection.SetLeader(proxy.GetLeaderId());
				
			} catch (TException e3) {
				m_oLogger.Error(m_oLogger.StackTraceToString(e3));
			}
			
			ByteBuffer buf;
			try {
				buf = proxy.GetMembershipList();
			} catch (TException e1) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e1));
				return Commons.FAILURE;
			}
			byte[] bufArr = new byte[buf.remaining()];
			buf.get(bufArr);
			try {
				m_oMember.MergeList(bufArr);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return Commons.FAILURE;
			}
		}
		return Commons.SUCCESS;
	}
	
	private int RecoverFromCheckPoint()
	{
		
		File fCP = new File(m_oConfig.GetCPPath());
		if(fCP.exists())
		{
			System.out.println("Checkpoint data found. Do you want to recover? Yes/No");
			String sInput = m_oUserInput.nextLine();
			if(sInput.equalsIgnoreCase("yes"))
			{
				try {
					BufferedReader brCP = new BufferedReader(new FileReader(m_oConfig.GetCPPath()));
					String text = brCP.readLine();
					String[] strArray = text.split(",");
					m_oHeartbeat.SendHB(strArray);
					for (String sIP: strArray) {  
						CommandIfaceProxy proxy = new CommandIfaceProxy();
						if(Commons.SUCCESS == proxy.Initialize(sIP, m_oConfig.CmdPort(), m_oLogger))
						{
							int counter = 0;
							while(!proxy.IsLeaderAlive())
							{
								if( counter++ > 10) 
								{
									m_oLogger.Error("Failed to receive leader from selected node.");
									//Breaking from this while loop
									break;
								}
								
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e) {
									m_oLogger.Error(m_oLogger.StackTraceToString(e));
								}
								m_oLogger.Warning("Leader not alive now. Trying in 5 secs");									
							}
							m_oElection.SetLeader(proxy.GetLeaderId());
							//Breaking from for loop
							break;
						}
							
					}
					
					// reset the serial number in intorducer object from checkpoint
					brCP = new BufferedReader(new FileReader(m_oConfig.GetCPPath() + "_SNO"));
					int s_no = Integer.parseInt(brCP.readLine());
					m_oIntroducer.setSno(s_no);
				} catch (FileNotFoundException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					return Commons.FAILURE;
				} catch (IOException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					return Commons.FAILURE;
				} catch (TException e) {
					m_oLogger.Warning(m_oLogger.StackTraceToString(e));
				}
			} else if(sInput.equalsIgnoreCase("no"))
			{
				System.out.println("Not recovering");
				// do nothing as of now
			} else
			{
				System.out.println("Invalid input. Not recovering");
			}
		}
		
		return Commons.SUCCESS;
	}
	
	public void WaitForServicesToEnd()
	{
		m_oCommServ.WaitCmdServiceToStop();
		
		
		m_oCommServ.WaitForHBRecvrToStop();
		
		try {
			m_HBThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		}
		
		try {
			m_FailDetThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		}
		
	}
	
	public void UserInputImpl()
	{
		Thread m_oUserInputThrd = new Thread(new Runnable() {           
            public void run() { 
            	while(true)
            	{
	            	System.out.println("=========================");
	            	System.out.println("Enter choice");
	        		System.out.println("1. Print Membership List");
	        		System.out.println("2. Leave");
	        		System.out.println("3. Print Node information");
	        		System.out.println("Enter Input ");
	        		String sInput = m_oUserInput.nextLine();
	        		if( ! sInput.equals("1") && !sInput.equals("2") && !sInput.equals("3"))
	        		{
	        			System.out.println("Invalid input");
	        			continue;
	        		}
	        		
	        		if(sInput.equals("1"))
	        		{
	        			m_oMember.PrintList();
	        		} else if(sInput.equals("2"))
	        		{
	        			LeaveList();
	        		} else if(sInput.equals("3"))
	        		{
	        			System.out.println("IP: " + hostIP );
	        			System.out.println("NodeType: " + m_sNodeType );
	        			System.out.println("Unique ID: " + m_oMember.UniqueId() );
	        		}
	        		try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
					}
            	}
        	} 
        });
		m_oUserInputThrd.start();
	}
	
}
