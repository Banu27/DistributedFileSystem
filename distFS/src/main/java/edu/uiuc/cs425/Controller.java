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
	private Logger 			m_oLogger;
	private static final String sLogPath = "/Users/anirudhnair/mp2/log/log.txt";
	private Scanner 		m_oUserInput;
	private String 			introIP;
	private String 			hostIP;
	
	public Controller()
	{
		m_oConfig 		= new ConfigAccessor();
		m_oCommServ		= new CommServer();
		m_oMember       = new Membership();
		m_oIntroducer   = null;
		m_oHeartbeat    = new Heartbeat();
		m_oLogger		= new Logger();
		m_oUserInput    = new Scanner(System.in);	
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
		
		m_oMember.Initialize(m_oConfig, m_oLogger);
		
		if(m_sNodeType.equals(Commons.NODE_INTROCUDER))
		{
			//Set membership obj in introducer
			
			m_oIntroducer = new Introducer(m_oMember,m_oLogger);

			if( Commons.FAILURE == m_oCommServ.Initialize(m_oConfig.HeartBeatPort(), 
					m_oMember, m_oIntroducer,m_oLogger) )
			{
				m_oLogger.Error("Failed to initialize the communication server");
				return Commons.FAILURE;
			}
		} else 
		{
			if( Commons.FAILURE == m_oCommServ.Initialize(m_oConfig.HeartBeatPort(), 
					m_oMember, m_oLogger))
			{
				m_oLogger.Error("Failed to initialize the communication server");
				return Commons.FAILURE;
			}
		}
		
		if( Commons.FAILURE == m_oHeartbeat.Initialize(m_oMember, m_oConfig, m_oLogger))
		{
			m_oLogger.Error("Failed to initialize the heartbeat sender");
			return Commons.FAILURE;
		}
		return Commons.SUCCESS;
	}
	

	
	public void StartAllServices()
	{
		if( m_sNodeType.equals(Commons.NODE_INTROCUDER))
		{
			m_oCommServ.StartIntroService(m_oConfig.IntroducerPort());
		}
		// bring up the heartbeat receiver
		m_oCommServ.StartHeartBeatRecvr();
	}
	
	public void StartHB()
	{
		// start heart beating thread
		m_HBThread = new Thread(m_oHeartbeat);
		m_HBThread.start();
				
				
	}
	
	public void StartFailureDetection()
	{
		//start failure detection thread
		m_FailDetThread = new Thread(m_oMember);
		m_FailDetThread.start();
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
		m_oMember.AddSelf();
		if(m_sNodeType.equals(Commons.NODE_PARTICIPANT))
		{
			MemberIntroProxy proxy = new MemberIntroProxy();
			int counter = 0;
			// continous pinging for introducer to connect
			while(Commons.FAILURE == proxy.Initialize(m_oConfig.IntroducerIP(), m_oConfig.IntroducerPort(), m_oLogger))
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
		
		// recover from checkpoint
		RecoverFromCheckPoint();
		return Commons.SUCCESS;
	}
	
	private int RecoverFromCheckPoint()
	{
		if(m_sNodeType.equals(Commons.NODE_INTROCUDER))
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
					} catch (FileNotFoundException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
						return Commons.FAILURE;
					} catch (IOException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
						return Commons.FAILURE;
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
		}
		return Commons.SUCCESS;
	}
	
	public void WaitForServicesToEnd()
	{
		if( m_sNodeType == Commons.NODE_INTROCUDER)
		{
			m_oCommServ.WaitForIntroServiceToStop();
		}
		
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
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	}
        	} 
        });
		m_oUserInputThrd.start();
	}
	
}
