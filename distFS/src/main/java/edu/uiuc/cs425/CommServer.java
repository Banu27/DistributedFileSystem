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
	
	private CommandIfaceImpl 		m_oIntroImpl;
	private HeartBeatReceiver       m_oHBRecvr;
	private Thread 					m_oIntroServThread;
	private Thread 					m_oHBRecvrThread;
	private Logger					m_oLogger;
	
	public int Initialize(int nHBPort, Membership oMember, Introducer oIntroducer, Logger oLogger)
	{
		m_oIntroImpl 		= new CommandIfaceImpl(); //Why does the Introducer not have a hb recvr??
		m_oLogger			= oLogger;
		if( Commons.FAILURE == m_oIntroImpl.Initialize())
		{
			oLogger.Error("Failed to initialize the the thrift introducer");
			return Commons.FAILURE;
		}
		m_oIntroImpl.SetIntoObj(oIntroducer);
		
		//Calling other Initialize
		Initialize(nHBPort,oMember,m_oLogger);
		return Commons.SUCCESS;
	}
	
	
	public int Initialize(int nHBPort, Membership oMember, Logger oLogger)
	{
		m_oLogger			= oLogger;
		m_oHBRecvr 			= new HeartBeatReceiver();
		if( Commons.FAILURE == m_oHBRecvr.Initialize(nHBPort,m_oLogger))
		{
			m_oLogger.Error("Failed to initialize the heartbeat receiver");
			return Commons.FAILURE;
		}
		m_oHBRecvr.SetMembershipObj(oMember);
		
		m_oIntroServThread  = null;
		m_oHBRecvrThread    = null;
		return Commons.SUCCESS;
	}
	
	public void StartIntroService(final int nPort)
	{
		m_oIntroServThread = new Thread(new Runnable() {           
            public void run() { 
            	try {
            		TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(nPort);
        		    TServer server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).processor(new CommandInterface.Processor(m_oIntroImpl)));
        		    server.serve();
        		} catch (TException e)
        		{
        			m_oLogger.Error(m_oLogger.StackTraceToString(e));
        		}
        		return;
        	} 
        });
		m_oIntroServThread.start();
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
	
	public void WaitForIntroServiceToStop()
	{
		try {
			m_oIntroServThread.join();
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
