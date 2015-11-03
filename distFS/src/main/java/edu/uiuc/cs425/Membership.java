package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.ByteArrayOutputStream;

import edu.uiuc.cs425.MembershipList.Member;
import edu.uiuc.cs425.MembershipList.MemberList;

public class Membership implements Runnable{
	
	private HashMap<String,MembershipListStruct> 		m_oHmap;
	private long  										m_nTfail;
	private String 										m_sIP;
	private String 										m_sUniqueId;
	private int											m_nUniqueSerialNumber;
	private int 										m_nMyHeartBeat;
	private ReentrantReadWriteLock 						m_oReadWriteLock; 
	private Lock 										m_oLockR; 
	private Lock 										m_oLockW; 
	private Logger										m_oLogger;
	private int											m_nFailChk;
	private PrintWriter								    m_oWriter;
	private ConfigAccessor 								m_oAccessor;
	private	Election									m_oElection;
	
	public String UniqueId()
	{
		return m_sUniqueId;
	}
	
	public void setElectionObject(Election electionObject)
	{
		m_oElection = electionObject;
	}
	
	
	public int Initialize(ConfigAccessor oAccessor, Logger logger, String introducerIP)
	{
		m_oAccessor = oAccessor;
		m_oHmap 		= new HashMap<String, MembershipListStruct>();
		m_nMyHeartBeat  = 0;
		m_nFailChk = 0;
		m_oReadWriteLock = new ReentrantReadWriteLock();
		m_oLockR = m_oReadWriteLock.readLock();
		m_oLockW = m_oReadWriteLock.writeLock();
		m_nTfail = m_oAccessor.FailureInterval();
		m_oLogger = logger;
		
		try {
			m_sIP  = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
			return Commons.FAILURE;
		}
		
		m_oLogger.Info(new String("Started Membership class"));
		
		return Commons.SUCCESS;
	}
		
	public void AddSelf(int serialNumber)
	{
		//Write lock. Add self called from controller
		//m_nSerialNumber = serialNumber;
		m_nUniqueSerialNumber = serialNumber;
		m_sUniqueId = new String(m_sIP + ":" + String.valueOf(GetMyLocalTime()));
		m_oLockW.lock();
		AddMemberToStruct( m_sUniqueId, m_sIP, m_nMyHeartBeat, GetMyLocalTime(), serialNumber);
		m_oLockW.unlock();
		m_oLogger.Info(new String("Added self node with id : " + m_sUniqueId));
	}
	
	public void AddMemberToStruct(String uniqueId, String IP, int heartbeatCounter, long localTime, int serialNumber)
	{
		//No write lock. Write lock present in Merge.
		MembershipListStruct newMember = new MembershipListStruct(IP, uniqueId, heartbeatCounter, localTime, serialNumber);
		m_oHmap.put(uniqueId,newMember);
		m_oLogger.Info(new String("IMPORTANT : Added new member to current memberlist : " + uniqueId));
	}
	
	public void IncrementHeartbeat()
	{
		if(m_oHmap.containsKey(m_sUniqueId))
		{
			m_nMyHeartBeat = m_nMyHeartBeat + 1;
			m_oHmap.get(m_sUniqueId).ResetHeartbeatCounter(m_nMyHeartBeat);
			m_oHmap.get(m_sUniqueId).ResetLocalTime(GetMyLocalTime());
	
		}
	}
	
	
	public MemberList CreateObject()
	{	 
		//Read lock
		m_oLockR.lock();
		MemberList.Builder memberListBuilder  = MemberList.newBuilder();
		List<Member> memberList = new ArrayList<Member>();
		Set<Entry<String, MembershipListStruct>> set = m_oHmap.entrySet();
	    Iterator<Entry<String, MembershipListStruct>> iterator = set.iterator();
	    while(iterator.hasNext()) {
	         Map.Entry mentry = (Map.Entry)iterator.next();
	         MembershipListStruct memberStruct = (MembershipListStruct) mentry.getValue(); //m_oHmap.get(mentry.getKey());
	         if(!memberStruct.IsSuspect())
	         {	Member.Builder member = Member.newBuilder();
	         	member.setHeartbeatCounter(memberStruct.GetHeartbeatCounter());
	         	member.setIP(memberStruct.GetIP());
	         	member.setUniqueSerialNumber(memberStruct.GetUniqueSerialNumber());
	         	member.setHasLeft(memberStruct.HasLeft());
	         	member.setLocalTime(memberStruct.GetLocalTime());
	         	member.setUniqueId(memberStruct.GetUniqueId());
	         	memberList.add(member.build());
	         }
	    }	      
		memberListBuilder.addAllMember(memberList);
		m_oLockR.unlock();
		return memberListBuilder.build();
		
	}
		
	public byte [] GetMemberList() throws Exception 
	{	
		return ObjectToByteBuffer(CreateObject());
	}
	
	public int MergeList(byte [] incomingListBuffer) throws Exception
	{
		MemberList incomingList = ObjectFromByteBuffer(incomingListBuffer);
		
		m_oLogger.Info(new String("Merging list")); //Needed?
		//Write lock 
		m_oLockW.lock();
		for(Member incomingMember : incomingList.getMemberList())
		{ 
			if(m_oHmap.containsKey(incomingMember.getUniqueId()))
			{
				MembershipListStruct matchedMember = m_oHmap.get(incomingMember.getUniqueId());
				//> Can never happen for self
				if(incomingMember.getHasLeft())
				{
					matchedMember.setAsLeft();
					m_oLogger.Info("IMPORTANT : " + matchedMember.GetUniqueId() + " has left");
					//WHY DID WE DECIDE TO DO THIS?
					//matchedMember.ResetLocalTime(GetMyLocalTime());
				}
				if(!matchedMember.HasLeft() 
						&& incomingMember.getHeartbeatCounter() > matchedMember.GetHeartbeatCounter())
				{
					matchedMember.ResetHeartbeatCounter(incomingMember.getHeartbeatCounter());
					matchedMember.ResetLocalTime(GetMyLocalTime());
					if(matchedMember.IsSuspect())
					{
						m_oLogger.Info(new String("IMPORTANT : Setting suspected node as Alive : " + matchedMember.GetUniqueId()));
						matchedMember.setAsAlive();
					}
				}
			}
			else
			{
				//Unseen member
				if(!incomingMember.getHasLeft())
				{	
					m_oLogger.Info("Adding node to memberlist " + incomingMember.getIP() );
					String IP = incomingMember.getIP();
					int heartbeatCounter = incomingMember.getHeartbeatCounter();
					int serialNumber = incomingMember.getUniqueSerialNumber();
					long localTime = GetMyLocalTime(); //Our machine localTime
					String uniqueId = incomingMember.getUniqueId();
					AddMemberToStruct(uniqueId, IP, heartbeatCounter, localTime, serialNumber);
				}
			}
		}
		m_oLockW.unlock();
		PrintListLogger(); 
		return Commons.SUCCESS;
	}
	
	public void PrintList() // only reading the list
	{
		m_oLockR.lock();
		ArrayList<String> vMembers = GetMemberIds();
		System.out.println("=============================");
		for(int i=0; i<vMembers.size(); ++i)
		{
			if(!m_oHmap.get(vMembers.get(i)).HasLeft())
				m_oHmap.get(vMembers.get(i)).Print();
		}
		m_oLockR.unlock();
		System.out.println("Current Leader Id : " + m_oElection.GetLeaderId());
		System.out.println("=============================");
	}
    
	public void PrintListLogger() // only reading the list
	{
		m_oLockR.lock();
		ArrayList<String> vMembers = GetMemberIds();
		StringBuffer msg = new StringBuffer();
		msg.append("============LIST=============\n");
		for(int i=0; i<vMembers.size(); ++i)
		{
			if(!m_oHmap.get(vMembers.get(i)).HasLeft())
				msg.append(m_oHmap.get(vMembers.get(i)).GetStr());
		}
		m_oLockR.unlock();
		msg.append("The leader is : " + m_oElection.GetLeaderId());
		msg.append("===========ENDLIST============");
		m_oLogger.Debug(msg.toString());
	}
	
	public long GetMyLocalTime()
	{
		return new Date().getTime();
	}
	
	public byte[] ObjectToByteBuffer(MemberList membershipList) throws Exception 
	{
		return membershipList.toByteArray();
	}
	
	public MemberList ObjectFromByteBuffer(byte[] buffer) throws Exception 
	{
		return MemberList.parseFrom(buffer);  
	}
	
	public String GetIP(String uniqueId)
	{
		return m_oHmap.get(uniqueId).GetIP();
	}
	
	public void run()
	{
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
		}
		ArrayList<String> sIps = new ArrayList<String>();
		while(true) {
			m_nFailChk++;
			long start_time = System.nanoTime();
			Set<Entry<String, MembershipListStruct>> set = m_oHmap.entrySet();
		    Iterator<Entry<String, MembershipListStruct>> iterator = set.iterator();
		    m_oLogger.Info("BENCHMARK: Failure check count: " + String.valueOf(m_nFailChk));
		    if(m_nFailChk % 5 == 0)
		    	sIps.clear();
		    while(iterator.hasNext()) {
		    
		    	//write lock since the members could be removed or set as suspect
		    	Map.Entry mentry = (Map.Entry)iterator.next();
		        MembershipListStruct memberStruct = (MembershipListStruct) mentry.getValue(); //m_oHmap.get(mentry.getKey());
		        
		        if(!memberStruct.GetUniqueId().equals(m_sUniqueId))
				{
		        	sIps.add(memberStruct.GetIP());
					if((memberStruct.IsSuspect() || memberStruct.HasLeft()) 
							&& ((GetMyLocalTime() - memberStruct.GetLocalTime()) > 2*m_nTfail))
					{	
						m_oLogger.Info(new String("IMPORTANT : Removing node : " + memberStruct.GetIP())); //UniqueId instead?
						m_oLogger.Info(new String("Unique id of failed node : " + memberStruct.GetUniqueId()));
						m_oLogger.Info(new String("Leader id : " + m_oElection.GetLeaderId()));
						if(memberStruct.GetUniqueId().equals(m_oElection.GetLeaderId()))
						{
							m_oLogger.Info(new String("Starting new election, leader failure detected"));
							m_oElection.StartElection();
							//Send a message to election Object????? and start election! Call from here??
						
						}
						m_oLockW.lock();
						iterator.remove();
						m_oLockW.unlock();
					}
					else
					{
						if((GetMyLocalTime() - memberStruct.GetLocalTime()) > m_nTfail && !memberStruct.HasLeft())
						{
							m_oLogger.Info(new String("IMPORTANT : Suspected node : " + memberStruct.GetIP()));
							memberStruct.setAsSuspect();
						}
					}
					if(memberStruct.IsSuspect())
					{
						m_oLogger.Info("The suspected node is : " + memberStruct.GetUniqueId());
						m_oLogger.Info("The heartbeat count of suspect node is : " + String.valueOf(memberStruct.GetHeartbeatCounter()));
				
					}
				}
			}
			//m_oLockW.unlock();
			long diff = (System.nanoTime() - start_time)/1000000;
			try {
				Thread.sleep(m_nTfail - diff);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return;
			}
			
			if(m_nFailChk % 5 == 0)
			{
				m_oLogger.Info("Checkpointing IPs");
				try {
					m_oWriter = new PrintWriter(m_oAccessor.GetCPPath(), "UTF-8");
					String ipCSV = sIps.toString();
					String ipCSVFinal = ipCSV.substring(1, ipCSV.length() - 1);
					m_oWriter.print(ipCSVFinal.replaceAll("\\s+",""));
				 	m_oWriter.close();				
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					m_oLogger.Error(m_oLogger.StackTraceToString(e1));
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					m_oLogger.Error(m_oLogger.StackTraceToString(e1));
				}
			}
			
		}
	}
	
	public int GetUniqueSerialNumber()
	{
		return m_nUniqueSerialNumber;
	}

	public int TimeToLeave()
	{
		m_oHmap.get(m_sUniqueId).setAsLeft();
		m_oLogger.Info(new String("IMPORTANT : Setting node as LEFT : " + m_sIP));
		return Commons.SUCCESS;
	}
	
	//Read lock
	 public ArrayList<String> GetMemberIds() 
	 {
		 m_oLockR.lock();
		 ArrayList<String> keyList = new ArrayList<String>(m_oHmap.keySet());
		 m_oLockR.unlock();
		 return keyList;
	 }
	 
	 //Read lock
	 public HashMap<Integer,String> GetSNoListAndIPList()
	 {
		 HashMap<Integer, String> hmap = new HashMap<Integer, String>();
		 m_oLockR.lock();
		 Set<Entry<String, MembershipListStruct>> set = m_oHmap.entrySet();
		 Iterator<Entry<String, MembershipListStruct>> iterator = set.iterator();
		 while(iterator.hasNext()) {
	         Map.Entry mentry = (Map.Entry)iterator.next();
	         MembershipListStruct memberStruct = (MembershipListStruct) mentry.getValue(); //m_oHmap.get(mentry.getKey());
	         hmap.put(memberStruct.GetUniqueSerialNumber(),memberStruct.GetIP());
	     }
		 m_oLockR.unlock();
		 return hmap;
	 }
	 
}
