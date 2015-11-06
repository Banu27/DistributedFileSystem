package edu.uiuc.cs425;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

public class ReplicationMgr  implements Runnable{

	private NodeFileMgr							m_oNodeProxy; //(FileManagerAtNode)
	private SDFSMaster 							m_oMaster;
	private Logger								m_oLogger;
	private HashMap<String,HashSet<String>>		m_oFileLocationTable;
	private int									m_nNumberOfReplicas; //Get from config accessor?
	private Membership							m_oMembership;
	private int									m_nServicePort;
	
	//Methods:
	//CheckReplicas //Separate thread - Periodic check
	//MakeReplica(IP) //To some node saying make replica in the given IP

	public void Initialize(Logger oLogger, SDFSMaster oMaster, Membership oMember, int numberOfReplicas, int nPort)
	{
	
		//Initialize only if master
		m_oLogger = oLogger;
		m_oMaster = oMaster;
		m_oFileLocationTable = oMaster.GetFileLocationTable(); 
		m_nNumberOfReplicas = numberOfReplicas;
		m_oMembership = oMember;
		m_nServicePort = nPort;
	}


	public void run() {
		// TODO Auto-generated method stub
		try {
			Thread.sleep(3000); //Where is this time defined??
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}
		
		while(true) {
			m_oFileLocationTable = m_oMaster.GetFileLocationTable();
			long start_time = System.nanoTime();
			Iterator<Entry<String, HashSet<String>>> iterator = m_oFileLocationTable.entrySet().iterator();
			while(iterator.hasNext())
			{
				Entry<String, HashSet<String>> element = iterator.next();
				String Filename = element.getKey();
				if(element.getValue().size() != m_nNumberOfReplicas)
				{
					int numberOfReplicasToBeMade = element.getValue().size() - m_nNumberOfReplicas;
					Random randNumberGenerator = new Random();
					ArrayList<String> IDs = m_oMembership.GetMemberIds();
					String idPresent; //Should be a list instead
					for(String id : element.getValue())
					{
						id = idPresent;
						IDs.remove(id);
					}
					for(int i=0; i<numberOfReplicasToBeMade; i++)
					{
						String copyIp = m_oMembership.GetIP(IDs.get(randNumberGenerator.nextInt(IDs.size())));
						CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
						if(Commons.SUCCESS == ProxyTemp.Initialize(idPresent, m_nServicePort, m_oLogger))
						{
							ProxyTemp.RequestFileCopy(Filename,copyIp);
						}
						//Call Add file here
					}
				}
				
			}
			
	}
	
	
}