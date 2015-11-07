package edu.uiuc.cs425;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;

import org.apache.thrift.TException;

public class SDFSClient {
	// get the master during every request from the introducer
	// make relevent calls to the master and make all the IO 
	// changes to the local directories
	
	private CommandIfaceProxy m_oIntoProxy;
	private CommandIfaceProxy m_oMasterProxy;
	private Logger			  m_oLogger;
	private ConfigAccessor    m_oConfig;
	
	SDFSClient(Logger logger, ConfigAccessor oAccessor)
	{
		m_oLogger 		= logger;
		m_oConfig 		= oAccessor;
		m_oIntoProxy 	= new CommandIfaceProxy();
		m_oMasterProxy  = new CommandIfaceProxy();
	}
	
	private int UpdateProxies() 
	{
		int counter = 0;
		// continuous pinging for introducer to connect
		while(Commons.FAILURE == m_oIntoProxy.Initialize(m_oConfig.IntroducerIP(), m_oConfig.CmdPort(), m_oLogger))
		{
			if( counter++ > 100) 
			{
				m_oLogger.Error("Failed to connect to Introducer. Exiting after 100 tries");
				return Commons.FAILURE;
			}
			
			// sleep 5 secs before next retry
			m_oLogger.Warning("Failed to connect to Introducer. Trying again in 5 secs");
			System.out.println("Failed to connect to Introducer. Trying again in 5 secs");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return Commons.FAILURE;
			}
		}
		//connected to introducer
		int leaderCounter = 0;
		try {
			while(!m_oIntoProxy.IsLeaderAlive())
			{
				if( leaderCounter++ > 10) 
				{
					m_oLogger.Error("Failed to receive leader. Exiting after 10 tries");
					return Commons.FAILURE;
				}
				m_oLogger.Warning(new String("Leader not alive. Trying in 5 secs"));
				System.out.println("Leader not alive. Trying in 5 secs");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				}
				
			}
		} catch (TException e1) {
			e1.printStackTrace();
			return Commons.FAILURE;
		}
		int leaderInit = 0;
		try {
			while(Commons.FAILURE == m_oMasterProxy.Initialize(m_oIntoProxy.GetLeaderIP(), m_oConfig.CmdPort(), m_oLogger))
			{
				if( leaderInit++ > 10) 
				{
					m_oLogger.Error("Failed to initialize leader proxy. Exiting after 10 tries");
					return Commons.FAILURE;
				}
				
				m_oLogger.Warning(new String("Failed to initialize leader proxy. Trying in 1 secs"));
				System.out.println("Failed to initialize leader proxy. Trying in 1 secs");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					return Commons.FAILURE;
				}
				
			}
		} catch (TException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		}
		return Commons.SUCCESS;
	}
	
	public int AddFile(String localPath, String SDFSName)
	{
		m_oLogger.Info("Addfile(): Updating proxy");
		if(UpdateProxies() == Commons.FAILURE)
		{
			System.out.println("Unable to connect to SDFS");
			return Commons.FAILURE;
		}
		m_oLogger.Info("Addfile(): Finished Updating proxy");
		m_oLogger.Info("Addfile(): Making the call to master for node info");
		// make call to master
		String sIP;
		try {
			sIP = m_oMasterProxy.RequestAddFile(SDFSName);
		} catch (TException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		}
		// make call to the node
		m_oLogger.Info("Addfile(): Creating the proxy to node " + sIP);
		CommandIfaceProxy proxy = new CommandIfaceProxy();
		if( Commons.FAILURE == proxy.Initialize(sIP, m_oConfig.CmdPort(), m_oLogger))
		{
			System.out.println("Unable to connect to " + sIP + " to add file. Try again");
			return Commons.FAILURE;
		}
		m_oLogger.Info("Addfile(): Creating a string buf of the file");
		ByteBuffer payload = null;
		FileInputStream fIn;
		FileChannel fChan;
		int fSize;
        try {
			fIn = new FileInputStream(localPath);
		
		    fChan = fIn.getChannel();
			fSize = (int) fChan.size();
			payload = ByteBuffer.allocate((int) fSize);
			fChan.read(payload);
			payload.rewind();
			fChan.close(); 
			fIn.close();
        } catch (FileNotFoundException e) {
        	m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		} catch (IOException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		}
        m_oLogger.Info("Addfile(): Transfer buffer to node");
		try {
			int retVal = proxy.AddFile(fSize, SDFSName, payload, true);
		} catch (TException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		}
		m_oLogger.Info("Addfile(): Add file complete");
		return Commons.SUCCESS;
	}
	
	public void GetFile(String SDFSName, String localPath)
	{
		m_oLogger.Info("GetFile(): Updating proxy");
		if(UpdateProxies() == Commons.FAILURE)
		{
			System.out.println("Unable to connect to SDFS");
			return;
		}
		m_oLogger.Info("GetFile(): Finished updating proxy");
		//ask master for the nodes to get the file from
		// make call to master
		m_oLogger.Info("GetFile(): Asking master for the file location");
		Set<String> sIPs;
		try {
			sIPs = m_oMasterProxy.GetFileLocations(SDFSName);
			
		} catch (TException e) {
			System.out.println("Unable to connect to leader. Try Again");
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return;
		}
		
		// make call to the node
		for(String sIP: sIPs)
		{
			m_oLogger.Info("GetFile(): Call to " + sIP + " to retrieve the file");
			CommandIfaceProxy proxy = new CommandIfaceProxy();
			if( Commons.SUCCESS == proxy.Initialize(sIP, m_oConfig.CmdPort(), m_oLogger))
			{
				try {
					ByteBuffer buf = proxy.GetFile(SDFSName);
					FileOutputStream fos;
					try {
						//write file to the localFS
						fos = new FileOutputStream(localPath);
						WritableByteChannel channel = Channels.newChannel(fos);
						channel.write(buf);
						channel.close();
					} catch (FileNotFoundException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
						continue;
					} catch (IOException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
						continue;
					}
					break;
				} catch (TException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					continue;
				}
				
				
				
			}
		}
		m_oLogger.Info("GetFile(): File copied to local filesystem");
	}
	
	public void DeleteFile(String SDFSName)
	{
		if(UpdateProxies() == Commons.FAILURE)
		{
			System.out.println("Unable to connect to SDFS");
			return;
		}
		
		try {
			m_oMasterProxy.DeleteFile(SDFSName);
		} catch (TException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			System.out.println("Connection error while deleting files");
		}
	}
	
	public void FilesAt(String sIP)
	{
		if(UpdateProxies() == Commons.FAILURE)
		{
			System.out.println("Unable to connect to SDFS");
			return;
		}
		// make call to the node
		CommandIfaceProxy proxy = new CommandIfaceProxy();
		if( Commons.FAILURE == proxy.Initialize(sIP, m_oConfig.CmdPort(), m_oLogger))
		{
			System.out.println("Unable to connect to " + sIP + ". Node may be down. Try again");
			return;
		}
		try {
			Set<String> files = proxy.GetFileList();
			if(files.size() > 0)
				System.out.println(files);
			else
				System.out.println("No files in the node");
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void FileLoc(String SDFSFileName)
	{
		if(UpdateProxies() == Commons.FAILURE)
		{
			System.out.println("Unable to connect to SDFS");
			return;
		}
		
		try {
			Set<String> ips = m_oMasterProxy.GetFileLocations(SDFSFileName);
			System.out.println(ips);
			
		} catch (TException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			System.out.println("Connection error while deleting files");
		}
	}
}
