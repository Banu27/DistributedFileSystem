package edu.uiuc.cs425;
import edu.uiuc.cs425.CommandIfaceImpl;

import org.apache.thrift.TException;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TSimpleServer;

import edu.uiuc.cs425.HeartBeatReceiver;

public class CommServer {
	
	private CommandIfaceImpl 		m_oCommandImpl;
	private HeartBeatReceiver       m_oHBRecvr;
	private FileReportRcvr			m_oFRRecvr;
	private Thread 					m_oCmdServThread;
	private Thread 					m_oHBRecvrThread;
	private Thread 					m_oFRRecvrThread;
	private Logger					m_oLogger;
	
	public int Initialize(Membership oMember, Introducer oIntroducer, Election oElection, 
			SDFSMaster oMaster, NodeFileMgr nodeMgr, Logger oLogger, ConfigAccessor oAccessor)
	{
		m_oCommandImpl 		= new CommandIfaceImpl(); //Why does the Introducer not have a hb recvr??
		m_oLogger			= oLogger;
		if( Commons.FAILURE == m_oCommandImpl.Initialize())
		{
			oLogger.Error("Failed to initialize the the thrift introducer");
			return Commons.FAILURE;
		}
		m_oCommandImpl.SetIntoObj(oIntroducer);
		m_oCommandImpl.setElectionObj(oElection);
		m_oCommandImpl.SetMasterObject(oMaster);
		m_oCommandImpl.SetNMObj(nodeMgr);
		
		// heartbeat recvr
		m_oHBRecvr 			= new HeartBeatReceiver();
		if( Commons.FAILURE == m_oHBRecvr.Initialize(oAccessor.HeartBeatPort(),m_oLogger))
		{
			m_oLogger.Error("Failed to initialize the heartbeat receiver");
			return Commons.FAILURE;
		}
		m_oHBRecvr.SetMembershipObj(oMember);
		
		// filereport recvr
		m_oFRRecvr  	= new FileReportRcvr();
		if( Commons.FAILURE == m_oFRRecvr.Initialize(oAccessor.GetFRPort(),m_oLogger))
		{
			m_oLogger.Error("Failed to initialize the filereporter receiver");
			return Commons.FAILURE;
		}
		m_oFRRecvr.SetMasterObj(oMaster);
		
		m_oCmdServThread    = null;
		m_oHBRecvrThread    = null;
		m_oFRRecvrThread    = null;
		return Commons.SUCCESS;
	}
	
		
	public void StartCmdService(final int nPort)
	{
		m_oCmdServThread = new Thread(new Runnable() {           
            public void run() { 
            	try {
            		TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(nPort);
        		    TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(new CommandInterface.Processor(m_oCommandImpl)));
        		    server.serve();
        		} catch (TException e)
        		{
        			m_oLogger.Error(m_oLogger.StackTraceToString(e));
        		}
        		return;
        	} 
        });
		m_oCmdServThread.start();
	}
	
	public void StartHeartBeatRecvr()
	{
		m_oHBRecvrThread = new Thread(new Runnable() {           
            public void run() { 
            	try {
        			m_oHBRecvr.StartService();
        		} catch (Exception e)
        		{
        			m_oLogger.Error("Failed to start the heartbeat receiver");
        			m_oLogger.Error(m_oLogger.StackTraceToString(e));
        		}
        		return;
        	} 
        });
		m_oHBRecvrThread.start();
		
		
	}
	
	public void StartFileReportRecvr()
	{
		m_oFRRecvrThread = new Thread(new Runnable() {           
            public void run() { 
            	try {
        			m_oHBRecvr.StartService();
        		} catch (Exception e)
        		{
        			m_oLogger.Error("Failed to start the heartbeat receiver");
        			m_oLogger.Error(m_oLogger.StackTraceToString(e));
        		}
        		return;
        	} 
        });
		m_oFRRecvrThread.start();
	}
	
	public void WaitCmdServiceToStop()
	{
		try {
			m_oCmdServThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		}
	}
	
	public void WaitForHBRecvrToStop()
	{
		try {
			m_oHBRecvrThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		}
	}
}
