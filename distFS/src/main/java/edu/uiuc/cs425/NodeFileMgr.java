package edu.uiuc.cs425;

import java.io.File;
import java.io.IOException;
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

import org.apache.thrift.TException;

import edu.uiuc.cs425.BlockMsg.BlockReport;

/*
 *  This class stores the information of all the blocks
 *  available in a node. The class also starts a thread
 *  that periodically sends the block report to the leader
 */
public class NodeFileMgr implements Runnable {
	private HashMap<String,Block> 		DNTable;
	private Election 					oElection;
	private Membership					oMemberList;
	private Logger						oLogger;
	private ConfigAccessor				oAccesor;
	private String						sMyIP;
	private ReentrantReadWriteLock 						m_oReadWriteLock; 
	private Lock 										m_oLockR; 
	private Lock 										m_oLockW; 
	
	public int Initialize(Logger logger, ConfigAccessor accessor, String myIP)
	{
		oLogger 		= logger;
		oAccesor 		= accessor;
		sMyIP			= myIP;
		
		return Commons.SUCCESS;
	}
	
	public void setElectionObj(Election obj)
	{
		oElection = obj;
	}
	
	public void setMembershipObjt(Membership obj)
	{
		oMemberList = obj;
	}
	
	NodeFileMgr()
	{
		DNTable = new HashMap<String,Block>();
		m_oReadWriteLock = new ReentrantReadWriteLock();
		m_oLockR = m_oReadWriteLock.readLock();
		m_oLockW = m_oReadWriteLock.writeLock();
	}
	
	
	// this request is forwarded by the thrift service. We assume that the
	// file is already written to the disk before this call is done. All this
	// call does is to add the block information to the DNTable and see if 
	// there is a need to replicate. If there is a replicaiton request needed
	// them it forwards it to two other nodes randomly selected. The method is
	// partially implemented. Look at the TODO at the end of the method.
	public int AddBlock(Block block, boolean replicate)
	{
		m_oLockW.lock();
		DNTable.put(block.sBlockID, block);
		m_oLockW.unlock();
		if(replicate)
		{
			// get ips from membership list
			Vector<String> sIPs = oMemberList.GetIPList();
	        // remove self from sIPs
			sIPs.remove(sMyIP);
			Set<Integer> rands = Commons.RandomK(Math.min(2, sIPs.size()),sIPs.size(),oMemberList.GetMyLocalTime());
			int failcount = 0;
			for (Integer i : rands)
			{
				CommandIfaceProxy proxy = new CommandIfaceProxy();
				proxy.Initialize(sIPs.get(i), oAccesor.CmdPort(), oLogger);
				try {
					proxy.AddBlock(block.nSize, block.sBlockID, block.GetBuffer(), false);
				} catch (TException e) {
					e.printStackTrace();
					failcount++;
				} catch (IOException e) {
					e.printStackTrace();
					failcount++;
				}
			}
			oLogger.Error("Failed to create replicas for " + block.sBlockID + " failed replicas=" + Integer.toString(failcount));
			// TODO: try again to add files to fail count number of nodes.
			
		}
		return Commons.SUCCESS;
	}
	
	// best effort delete. This call is again forwarded from the thrift service.
	// If the block is present in the table then delete the file and remove the 
	// block from the table
	public void DeleteBlock(String sBlockID)
	{
		m_oLockR.lock();
		Block block = DNTable.get(sBlockID);
		m_oLockR.unlock();
		if( block != null)
		{
			File file = new File(block.sFilePath);
			if(!file.delete())
				oLogger.Error("Unable to delete " + block.sFilePath + ". Will remove the block from the DNTable");
			m_oLockW.lock();
			DNTable.remove(sBlockID);
			m_oLockW.unlock();
			oLogger.Info("Removed Block: " + sBlockID + "from the DNTable");
		} else {
			oLogger.Warning("Block not found for deletion: " + sBlockID);
		}
	}
	
	// converts the blockIDs in the node to a binary format using protobuf
	public byte[] GetBlockReport()
	{
		BlockReport.Builder reportBuilder = BlockReport.newBuilder();
		List<String> report = new ArrayList<String>();
		m_oLockR.lock();
		Set<String> keys = DNTable.keySet();
		m_oLockR.unlock();
		for(String key: keys)
			report.add(key);
		reportBuilder.addAllSBlockIDs(report);
		reportBuilder.setNodeID(oMemberList.UniqueId());
		BlockReport blockReport = reportBuilder.build();
		return blockReport.toByteArray();
	}
	
	
	// this call is invoked from the controller by starting a 
	// nre thread. 
	public void run() {
		try {
			Thread.sleep(oAccesor.GetBRInterval());
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			oLogger.Error(oLogger.StackTraceToString(e1));
		}
		while(true)
		{
			if(DNTable.size() > 0 && oElection.isMasterAlive())
			{
				BlockReportProxy proxy = new BlockReportProxy();
				// TODO: get current leader from the election obj
				String ip ="asd"; // from election obj
				proxy.Initialize(ip, oAccesor.GetBRPort(), oLogger);
				try {
					proxy.SendBlockReport(GetBlockReport());
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			
			try {
				Thread.sleep(oAccesor.GetBRInterval());
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				oLogger.Error(oLogger.StackTraceToString(e1));
			}
		}
	}
	
	
	
}
