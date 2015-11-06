package edu.uiuc.cs425;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.thrift.TException;

public class ReplicationMgr  implements Runnable{

	private NodeFileMgr							m_oNodeProxy; //(FileManagerAtNode)
	private SDFSMaster 							m_oMaster;
	private Logger								m_oLogger;
	private HashMap<String,HashSet<String>>		m_oFileLocationTable;
	private int									m_nNumberOfReplicas; //Get from config accessor?
	private Membership							m_oMembership;
	private int									m_nServicePort;
	private int									m_nFileReportInterval; //Get from config accessor
	
	//Methods:
	//CheckReplicas //Separate thread - Periodic check
	//MakeReplica(IP) //To some node saying make replica in the given IP

	public void Initialize(Logger oLogger, SDFSMaster oMaster, Membership oMember, int numReplicas, int nPort, int fileReportInterval)
	{
	
		//Initialize only if master
		m_oLogger = oLogger;
		m_oMaster = oMaster;
		m_oFileLocationTable = oMaster.GetFileLocationTable(); 
		m_nNumberOfReplicas = numReplicas;
		m_oMembership = oMember;
		m_nServicePort = nPort;
		m_nFileReportInterval = fileReportInterval;
	}


	public void run() {
		// TODO Auto-generated method stub
		
		//Initial sleep
		try {
			Thread.sleep(3000); //Where is this time defined??
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}
		
		while(true) {
			
			//First update the File Loc table
			m_oFileLocationTable = m_oMaster.GetFileLocationTable();
			
			//For interval uniformity
			long start_time = System.nanoTime();
			
			//Iterator for each element in File Loc
			Iterator<Entry<String, HashSet<String>>> iterator = m_oFileLocationTable.entrySet().iterator();
			while(iterator.hasNext())
			{
				//The element - Filename and containing node list
				Entry<String, HashSet<String>> element = iterator.next();
				String Filename = element.getKey();
				
				//Checking node list
				if(element.getValue().size() != m_nNumberOfReplicas && !element.getValue().isEmpty())
				{
					int numberOfReplicasToBeMade = element.getValue().size() - m_nNumberOfReplicas;
					Random randNumberGenerator = new Random();
					
					//Getting all the member ids so as to choose ids randomly for replication
					ArrayList<String> IDs = m_oMembership.GetMemberIds();
					
					//New vector to store existing ids
					Vector<String> idPresent = new Vector<String>(); //Should be a list instead
					
					for(String id : IDs)
					{
						idPresent.addElement(id);
						IDs.remove(id);
					}
					for(int i=0; i<numberOfReplicasToBeMade; i++)
					{
						//A random id where copy is to be made
						String copyIp = m_oMembership.GetIP(IDs.get(randNumberGenerator.nextInt(IDs.size())));
						CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
						
						//Copy is made from idPresent[0]
						if(Commons.SUCCESS == ProxyTemp.Initialize(idPresent.get(0), m_nServicePort, m_oLogger))
						{
							try {
								ProxyTemp.RequestFileCopy(Filename,copyIp);
							} catch (TException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						//Call Add file here
						
						//Removing the copyIp from the list to avoid repetition
						IDs.remove(copyIp);
					}
				}
				if(element.getValue().size() > m_nNumberOfReplicas)
				{
					HashSet<String> listOfIds = element.getValue();
					
					Iterator<String> iteratorElemToBeDeleted = listOfIds.iterator();
					
					while(listOfIds.size() > m_nNumberOfReplicas)
					{
						if(iteratorElemToBeDeleted.hasNext())
						{
							String id = iteratorElemToBeDeleted.next();
							//Delete file at iterator.next()
							
							CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
							
							//Copy is made from idPresent[0]
							if(Commons.SUCCESS == ProxyTemp.Initialize(m_oMembership.GetIP(id), m_nServicePort, m_oLogger))
								try {
									ProxyTemp.DeleteFile(Filename);
								} catch (TException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							listOfIds.remove(id);
							element.getValue().remove(id); 
						}
					}
					
				}
				
			}
			long diff = (System.nanoTime() - start_time)/1000000;
			try {
				Thread.sleep(m_nFileReportInterval - diff);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return;
			}
			
		}
	
	}	
}