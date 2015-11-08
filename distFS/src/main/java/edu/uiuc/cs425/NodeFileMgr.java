package edu.uiuc.cs425;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;

import edu.uiuc.cs425.FileMsg.FileReport;

/*
 *  This class stores the information of all the files
 *  available in a node. The class also starts a thread
 *  that periodically sends the file report to the leader
 */
public class NodeFileMgr implements Runnable {
	private HashMap<String,SDFSFile> 		DNTable;
	private Election 						m_oElection;
	private Membership						m_oMemberList;
	private Logger							m_oLogger;
	private ConfigAccessor					m_oAccesor;
	private String							m_sMyIP;
	private ReentrantReadWriteLock 						m_oReadWriteLock; 
	private Lock 										m_oLockR; 
	private Lock 										m_oLockW; 
	
	
	public NodeFileMgr()
	{
		DNTable 			= new HashMap<String,SDFSFile>();
		m_oReadWriteLock 	= new ReentrantReadWriteLock();
		m_oLockR 			= m_oReadWriteLock.readLock();
		m_oLockW 			= m_oReadWriteLock.writeLock();
	}
	
	public int Initialize(Logger logger, ConfigAccessor accessor, String myIP, Election election, Membership membership)
	{
		m_oLogger 		= logger;
		m_oAccesor 		= accessor;
		m_sMyIP			= myIP;
		m_oElection 	= election;
		m_oMemberList   = membership;
		// clean the data dir everytime
		try {
			FileUtils.cleanDirectory(new File(accessor.SDFSDataDir()));
		} catch (IOException e) {
			m_oLogger.Warning(m_oLogger.StackTraceToString(e));
		}
		
		return Commons.SUCCESS;
	}
	
	
	// this request is forwarded by the thrift service. We assume that the
	// file is already written to the disk before this call is done. All this
	// call does is to add the file information to the DNTable and see if 
	// there is a need to replicate. If there is a replication request needed
	// them it forwards it to two other nodes randomly selected. The method is
	// partially implemented. Look at the TODO at the end of the method.
	public int AddFile(SDFSFile file_, boolean replicate)
	{
		m_oLockW.lock();
		DNTable.put(file_.m_sFileID, file_);
		m_oLockW.unlock();
		//send ack to master
		if(m_oElection.IsLeaderAlive())
		{
			CommandIfaceProxy proxyTemp = new CommandIfaceProxy();
			m_oLogger.Info("Trying to initiate connection with " + m_oElection.GetLeaderIP() );
			if(Commons.SUCCESS == proxyTemp.Initialize(m_oElection.GetLeaderIP(),m_oAccesor.CmdPort(),m_oLogger))
			{	try {
					proxyTemp.FileStorageAck(file_.m_sFileID, m_oMemberList.UniqueId());; 
													
				} catch (TException e) {
					m_oLogger.Error("Failed to send ack message to " + m_oElection.GetLeaderIP());
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					
				}
			}
		}
		
		if(replicate)
		{
			// get ips from membership list
			ArrayList<String> vUniqueIds = m_oMemberList.GetMemberIds();
			String myID = m_oMemberList.UniqueId();
			vUniqueIds.remove(myID);
			Set<Integer> rands = Commons.RandomK(Math.min(m_oAccesor.GetReplicationFactor() - 1, vUniqueIds.size()),vUniqueIds.size(),m_oMemberList.GetMyLocalTime());
			int failcount = 0;
			for (Integer i : rands)
			{
				CommandIfaceProxy proxy = new CommandIfaceProxy();
				proxy.Initialize(m_oMemberList.GetIP(vUniqueIds.get(i)), m_oAccesor.CmdPort(), m_oLogger);
				try {
					proxy.AddFile(file_.m_nSize, file_.m_sFileID, file_.GetBuffer(), false);
				} catch (TException e) {
					m_oLogger.Warning(m_oLogger.StackTraceToString(e));
					failcount++;
				} catch (IOException e) {
					m_oLogger.Warning(m_oLogger.StackTraceToString(e));
					failcount++;
				}
			}
			if(failcount > 0)
				m_oLogger.Error("Failed to create replicas for " + file_.m_sFileID + " failed replicas=" + Integer.toString(failcount));
			// TODO: try again to add files to fail count number of nodes.
			
		}
		return Commons.SUCCESS;
	}
	
	// best effort delete. This call is again forwarded from the thrift service.
	// If the file is present in the table then delete the file and remove the 
	// file from the table
	public int DeleteFile(String sFileID)
	{
		m_oLockR.lock();
		SDFSFile file_ = DNTable.get(sFileID);
		m_oLockR.unlock();
		if( file_ != null)
		{
			File file = new File(file_.m_sFilePath);
			if(!file.delete())
				m_oLogger.Error("Unable to delete " + file_.m_sFilePath + ". Will remove the file from the DNTable");
			m_oLockW.lock();
			DNTable.remove(sFileID);
			m_oLockW.unlock();
			m_oLogger.Info("Removed File: " + sFileID + "from the DNTable");
			return Commons.SUCCESS;
		} else {
			m_oLogger.Warning("File not found for deletion: " + sFileID);
			return Commons.FAILURE;
		}
	}
	
	// converts the FileIDs in the node to a binary format using protobuf
	public byte[] GetFileReport()
	{
		FileReport.Builder reportBuilder = FileReport.newBuilder();
		List<String> report = new ArrayList<String>();
		m_oLockR.lock();
		Set<String> keys = DNTable.keySet();
		m_oLockR.unlock();
		for(String key: keys)
			report.add(key);
		reportBuilder.addAllSFileIDs(report);
		reportBuilder.setNodeID(m_oMemberList.UniqueId());
		FileReport fileReport = reportBuilder.build();
		return fileReport.toByteArray();
	}
	
	//THIS NEEDS TO BE DONE
	public void RequestFileCopy(String filename, String nodeIP)
	{
		
		CommandIfaceProxy proxy = new CommandIfaceProxy();
		
		
		if( Commons.SUCCESS == proxy.Initialize(nodeIP, m_oAccesor.CmdPort(), m_oLogger))
		{
			// read the file
			SDFSFile file_ = DNTable.get(filename);
			if(file_ != null)
			{
				try {
					proxy.AddFile(file_.m_nSize, file_.m_sFileID, file_.GetBuffer(), false);
				} catch (TException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
					
				} catch (IOException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				}
		
			} else {
				m_oLogger.Error("Unable to find file: " + filename + " in the node");
			}
		} else
		{
			m_oLogger.Error("Unable to connect to " + nodeIP + " make copy");
		}
		
		
		
	}
	
	public Set<String> GetFileList()
	{
		m_oLockR.lock();
		Set<String> keys =  DNTable.keySet();
		m_oLockR.unlock();
		return keys;
	}
	
	// this call is invoked from the controller by starting a 
	// new thread. 
	public void run() {
		try {
			Thread.sleep(m_oAccesor.GetFRInterval());
		} catch (InterruptedException e1) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}
		while(true)
		{
			if((DNTable.size() > 0) && m_oElection.IsLeaderAlive())
			{
				m_oLogger.Info("Sending file report to master");
				FileReportProxy proxy = new FileReportProxy();
				String ip = m_oElection.GetLeaderIP();
				proxy.Initialize(ip, m_oAccesor.GetFRPort(), m_oLogger);
				try {
					proxy.SendFileReport(GetFileReport());
				} catch (IOException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				}
			}
			
			try {
				Thread.sleep(m_oAccesor.GetFRInterval());
			} catch (InterruptedException e1) {
				m_oLogger.Error(m_oLogger.StackTraceToString(e1));
			}
		}
	}
	
	public ByteBuffer GetFile(String SDFSName)
	{
		SDFSFile file_ = DNTable.get(SDFSName);
		ByteBuffer mBuf = null;
		if( file_ != null )
		{
			FileInputStream fIn;
		    FileChannel fChan;
		    long fSize;

		    try {
		      fIn = new FileInputStream(m_oAccesor.SDFSDataDir() + SDFSName);
		      fChan = fIn.getChannel();
		      fSize = fChan.size();
		      mBuf = ByteBuffer.allocate((int) fSize);
		      fChan.read(mBuf);
		      mBuf.rewind();
		      fChan.close(); 
		      fIn.close();
		    } catch (IOException exc) {
		    	m_oLogger.Error(m_oLogger.StackTraceToString(exc));
		    	return null;
		      }
		}
		return mBuf;
	}
	
}
