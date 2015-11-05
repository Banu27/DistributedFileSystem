package edu.uiuc.cs425;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.thrift.TException;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.uiuc.cs425.FileMsg.FileReport;


public class SDFSMaster {
	
	//Hash set does not guarantee any order. 
	//This means we cannot say that the first value is primary copy etc.
	private HashMap<String,HashSet<String>> 		m_oFileLocationTable;
	private Membership 								m_oMembership;
	private Logger									m_oLogger;
	private String									m_sMyID;
	private ReentrantReadWriteLock 					m_oReadWriteLock; 
	private Lock 									m_oLockR; 
	private Lock 									m_oLockW; 
	private int										m_nCommandServicePort;
	
	public void Initialize(Membership membership, Logger logger, int servicePort)
	{
		m_oMembership = membership;
		m_oLogger = logger;
		m_nCommandServicePort = servicePort;
		m_sMyID = m_oMembership.UniqueId(); //Ensure this is working
		m_oReadWriteLock = new ReentrantReadWriteLock();
		m_oLockR = m_oReadWriteLock.readLock();
		m_oLockW = m_oReadWriteLock.writeLock();
		
	}
	
	public List<String> GetAvailableFiles() //List used because of thrift
	{
		m_oLockR.lock();
		List<String> FileList = new ArrayList<String>();
		Set<Entry<String, HashSet<String>>> iteratorSet = m_oFileLocationTable.entrySet();
		Iterator<Entry<String, HashSet<String>>> iterator = iteratorSet.iterator();
		while(iterator.hasNext())
		{
			Entry<String, HashSet<String>> currFile = iterator.next();
			//Printing the file name as available only if at least one copy is present
			if(!currFile.getValue().isEmpty())
				FileList.add(currFile.getKey());
		}
		m_oLockR.unlock();
		return FileList;
	}
	
	//Return an IP randomly?
	//FIX THIS IN THE IMPL AND PROXY
	public String RequestAddFile(String FileId) // thrift //What is payload??
	{
		//Populate values if acknowledgement is received
		m_oLockW.lock();
		m_oFileLocationTable.put(FileId, new HashSet<String>());
		m_oLockW.unlock();
		ArrayList<String> IDs = m_oMembership.GetMemberIds();
		
		String PrimaryIp = GetPrimaryId(IDs);
		while(!PrimaryIp.equals(String.valueOf(Commons.FAILURE)))
		{
			PrimaryIp = GetPrimaryId(IDs);
		}
		
		return PrimaryIp;
	}
	
	private String GetPrimaryId(ArrayList<String> IDs)
	{
		int NumOfMembers = IDs.size();
		Random randomNumberGenerator = new Random();
		int newRand;
		newRand = randomNumberGenerator.nextInt(NumOfMembers);
		while(IDs.get(newRand) == m_sMyID)
		{
			newRand = randomNumberGenerator.nextInt(NumOfMembers);
		}
		return m_oMembership.GetIP(IDs.get(newRand));
	}
	
	void DeleteFile(String Filename)	//Thrift
	{
		HashSet<String> NodeList = m_oFileLocationTable.get(Filename);
		Iterator<String> iterator = NodeList.iterator();
		while(iterator.hasNext())
		{
			//create proxy with iterator.next()
			//proxy.deleteFile(Filename);
			CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
			if(Commons.SUCCESS == ProxyTemp.Initialize(m_oMembership.GetIP(iterator.next()),m_nCommandServicePort,m_oLogger))
			{
				try {
					ProxyTemp.DeleteFile(Filename);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	
		}
		
	}
	
	//Initiate file transfer to client
	//File HAS to be present and available
	//Using set return type because of thrift and existing hashset in hashmap
	Set<String> GetFileLocations(String Filename) 	//Thrift
	{
		HashSet<String> NodeList = m_oFileLocationTable.get(Filename);
		return NodeList;		
	}
	
	//From the first file add - received from the primary copy	
	void FileStorageAck(String Filename, String IncomingID) // from client thrift
	{
		//FileStorageAck if only received after the primary copy is successful
		m_oFileLocationTable.get(Filename).add(IncomingID);
	}
	
	public void MergeReport(byte[] reportBuf) throws InvalidProtocolBufferException
	{
		FileReport report = FileReport.parseFrom(reportBuf);
		// TODO complete impl of the actual merge with the MAsterBlockLocTable
		// get the nodeID and Block info from the below calls
		//report.getNodeID();
		//report.getSBlockIDsList()
		
		m_oLogger.Info("Merging FileReport");
		
		//Write lock while merging
		m_oLockW.lock();
		String IncomingNodeId = report.getNodeID();
		List<String> FileIDs = report.getSFileIDsList();
		for(String incomingFileName : FileIDs)
		{ 
			if(m_oFileLocationTable.containsKey(incomingFileName))
			{
				HashSet<String> matchedFileNodeList = m_oFileLocationTable.get(incomingFileName);
				if(!matchedFileNodeList.contains(IncomingNodeId))
				{
					matchedFileNodeList.add(IncomingNodeId);
				}
				
			}
		}
		m_oLockW.unlock();
	}
}
