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


public class SDFSMaster implements Runnable {
	
	//Hash set does not guarantee any order. 
	//This means we cannot say that the first value is primary copy etc.
	private HashMap<String,HashSet<String>> 		m_oFileLocationTable;
	private Membership 								m_oMembership;
	private Logger									m_oLogger;
	private ReentrantReadWriteLock 					m_oReadWriteLock; 
	private Lock 									m_oLockR; 
	private Lock 									m_oLockW; 
	private int										m_nCommandServicePort;
	private Election								m_oElection;
	private ConfigAccessor							m_oConfig;
	private Thread          						m_oReplicationMgrThread;
	private NodeFileMgr								m_oNodeMgr;
	
	public SDFSMaster()
	{
		m_oFileLocationTable = new HashMap<String,HashSet<String>>();
		
	}
	
	public void StartReplicationMgr()
	{
		m_oReplicationMgrThread =  new Thread(this);
		m_oReplicationMgrThread.start();
	}
	
	
	public int Initialize(Membership membership, Logger logger, int servicePort, Election election,
			NodeFileMgr oNodeMgr, ConfigAccessor oConfig)
	{
		m_oMembership 				= membership;
		m_oLogger		 			= logger;
		m_nCommandServicePort 		= servicePort;
		m_oElection 				= election;
		m_oReadWriteLock 			= new ReentrantReadWriteLock();
		m_oLockR 					= m_oReadWriteLock.readLock();
		m_oLockW 					= m_oReadWriteLock.writeLock();
		m_oConfig					= oConfig;
		m_oNodeMgr					= oNodeMgr;
		return Commons.SUCCESS;
	}
	
	
	public int MasterSetup()
	{
		// I am going to make the master ask all the other members for block report
		// It requests all the nodes but reports but doesn't care if it hasn't received. Is this alright??
		// This is because, only 2 failures at a time. Assuming that the reports don't get sent only in a failure case,
		// this is alright because one id per file is always present. Not very robust?? I can make it TCP. 
		ArrayList<String> IDs = m_oMembership.GetMemberIds();
		
		Iterator<String> iterator = IDs.iterator();
		while(iterator.hasNext())
		{
			CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
			String nodeId = iterator.next();
			if(m_oMembership.GetIP(nodeId).equals(m_oMembership.GetIP(m_oMembership.UniqueId())))
			{
				Set<String> fileSetForID = m_oNodeMgr.GetFileList();
				for(String id : fileSetForID)
				{
					if(m_oFileLocationTable.containsKey(id))
					{
						m_oFileLocationTable.get(id).add(nodeId);
					}
					else
					{
						m_oFileLocationTable.put(id, new HashSet<String>());
						m_oFileLocationTable.get(id).add(nodeId);
					}
				}
				
			} else {
				if(Commons.SUCCESS == ProxyTemp.Initialize(m_oMembership.GetIP(nodeId), m_nCommandServicePort, m_oLogger))
				{
					try {
						//Receiving the set of filenames for each node
						Set<String> fileSetForID = ProxyTemp.GetFileList();
						for(String id : fileSetForID)
						{
							if(m_oFileLocationTable.containsKey(id))
							{
								m_oFileLocationTable.get(id).add(nodeId);
							}
							else
							{
								m_oFileLocationTable.put(id, new HashSet<String>());
								m_oFileLocationTable.get(id).add(nodeId);
							}
						}
					} catch (TException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
					}
				}
			}
		}
		
		// also start the replication manager
		StartReplicationMgr();
		
		return Commons.SUCCESS;
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
	
	public String RequestAddFile(String FileId) // thrift //What is payload??
	{
		m_oLogger.Info("Received request at MASTER to add file " + FileId );
		//Populate values (nodeList) if acknowledgement is received
		m_oLockW.lock();
		m_oFileLocationTable.put(FileId, new HashSet<String>());
		m_oLockW.unlock();
		ArrayList<String> IDs = m_oMembership.GetMemberIds();
		Random randomNumberGenerator = new Random();
		return m_oMembership.GetIP(IDs.get(randomNumberGenerator.nextInt(IDs.size())));
	}
				
	void DeleteFile(String Filename)	//Thrift
	{
		m_oLogger.Info("Received request at MASTER to delete file " + Filename );	
		Set<String> NodeList = GetFileLocations(Filename);
		m_oLogger.Info("Found node list: " + NodeList.toString());
		Iterator<String> iterator = NodeList.iterator();
		while(iterator.hasNext())
		{
			String sIP = m_oMembership.GetIP(iterator.next());
			m_oLogger.Info("========" + sIP  + "========");
			if(sIP.equals(m_oMembership.GetIP(m_oMembership.UniqueId())))
			{
				m_oNodeMgr.DeleteFile(Filename);
			} else {
				CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
				if(Commons.SUCCESS == ProxyTemp.Initialize(sIP,m_nCommandServicePort,m_oLogger))
				{
					try {
						ProxyTemp.DeleteFile(Filename);//The thrift thing has to return from this call.
					} catch (TException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
					}
				}	
			}
		}
		m_oFileLocationTable.remove(Filename);
	}
	
	//Initiate file transfer to client
	//File HAS to be present and available
	//Using set return type because of thrift and existing hashset in hashmap
	Set<String> GetFileLocations(String Filename) 	//Thrift
	{
		HashSet<String> nodeList = m_oFileLocationTable.get(Filename);
		Set<String> nodeIps = new HashSet<String>();
		for(String id: nodeList)
			nodeIps.add(m_oMembership.GetIP(id));
		return nodeIps;		
	}
	
	//From the first file add - received from the primary copy
	//Is it received from every node? 
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
	
	void CheckReplication() {
		// Initial sleep
		try {
			Thread.sleep(m_oConfig.GetReplicationCheckInterval()); // Where is this time defined??
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}

		while (true) {

			// For interval uniformity
			long start_time = System.nanoTime();

			// Iterator for each element in File Loc
			m_oLockR.lock();
			Iterator<Entry<String, HashSet<String>>> iterator = m_oFileLocationTable.entrySet().iterator();
			while (iterator.hasNext()) {
				// The element - Filename and containing node list
				Entry<String, HashSet<String>> element = iterator.next();
				String Filename = element.getKey();

				// Checking node list
				if (element.getValue().size() != m_oConfig.GetReplicationFactor() && !element.getValue().isEmpty()) {
					int numberOfReplicasToBeMade = element.getValue().size() - m_oConfig.GetReplicationFactor();
					Random randNumberGenerator = new Random();

					// Getting all the member ids so as to choose ids randomly
					// for replication
					ArrayList<String> IDs = m_oMembership.GetMemberIds();

					// New vector to store existing ids
					Vector<String> idPresent = new Vector<String>(); // Should
																		// be a
																		// list
																		// instead

					for (String id : IDs) {
						idPresent.addElement(id);
						IDs.remove(id);
					}
					for (int i = 0; i < numberOfReplicasToBeMade; i++) {
						// A random id where copy is to be made
						String copyIp = m_oMembership.GetIP(IDs.get(randNumberGenerator.nextInt(IDs.size())));
						CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();

						// Copy is made from idPresent[0]
						if (Commons.SUCCESS == ProxyTemp.Initialize(idPresent.get(0), m_oConfig.CmdPort(), m_oLogger)) {
							try {
								ProxyTemp.RequestFileCopy(Filename, copyIp);
							} catch (TException e) {
								m_oLogger.Error(m_oLogger.StackTraceToString(e));
							}
						}
						// Call Add file here

						// Removing the copyIp from the list to avoid repetition
						IDs.remove(copyIp);
					}
				}
				if (element.getValue().size() > m_oConfig.GetReplicationFactor()) {
					HashSet<String> listOfIds = element.getValue();

					Iterator<String> iteratorElemToBeDeleted = listOfIds.iterator();

					while (listOfIds.size() > m_oConfig.GetReplicationFactor()) {
						if (iteratorElemToBeDeleted.hasNext()) {
							String id = iteratorElemToBeDeleted.next();
							// Delete file at iterator.next()

							CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();

							// Copy is made from idPresent[0]
							if (Commons.SUCCESS == ProxyTemp.Initialize(m_oMembership.GetIP(id), m_oConfig.CmdPort(),
									m_oLogger))
								try {
									ProxyTemp.DeleteFile(Filename);
								} catch (TException e) {
									m_oLogger.Error(m_oLogger.StackTraceToString(e));
								}
							listOfIds.remove(id);
							element.getValue().remove(id);
						}
					}

				}

			}
			m_oLockR.unlock();
			long diff = (System.nanoTime() - start_time) / 1000000;
			try {
				Thread.sleep(m_oConfig.GetReplicationCheckInterval() - diff);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return;
			}

		}
	}

	public void run() {
		CheckReplication();
	}
}
