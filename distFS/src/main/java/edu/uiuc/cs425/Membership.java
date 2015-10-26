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
	private int 										m_nMyHeartBeat;
	private ReentrantReadWriteLock 						m_oReadWriteLock; 
	private Lock 										m_oLockR; 
	private Lock 										m_oLockW; 
	private Logger										m_oLogger;
	private int											m_nFailChk;
	private PrintWriter								    m_oWriter;
	private ConfigAccessor 								m_oAccessor;
	
	
	public String UniqueId()
	{
		return m_sUniqueId;
	}
	
	public int Initialize(ConfigAccessor oAccessor, Logger logger)
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
		
	public void AddSelf()
	{
		//Write lock. Add self called from controller
		//m_nSerialNumber = serialNumber;
		m_sUniqueId = new String(m_sIP + ":" + String.valueOf(GetMyLocalTime()));
		m_oLockW.lock();
		AddMemberToStruct( m_sUniqueId, m_sIP, m_nMyHeartBeat, GetMyLocalTime());
		m_oLockW.unlock();
		m_oLogger.Info(new String("Added self node with id : " + m_sUniqueId));
	}
	
	public void AddMemberToStruct(String uniqueId, String IP, int heartbeatCounter, long localTime)
	{
		//No write lock. Write lock present in Merge.
		MembershipListStruct newMember = new MembershipListStruct(IP, uniqueId, heartbeatCounter, localTime);
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
	         	member.setHasLeft(memberStruct.HasLeft());
	         	member.setLocalTime(memberStruct.GetLocalTime());
	         	member.setUniqueId(memberStruct.GetUniqueId());
	         	memberList.add(member.build());
	         }
	    }	      
		memberListBuilder.addAllMember(memberList);
		m_oLockR.unlock();
		//LOG????
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
		for(Member member : incomingList.getMemberList())
		{ 
			if(m_oHmap.containsKey(member.getUniqueId()))
			{
				MembershipListStruct matchedMember = m_oHmap.get(member.getUniqueId());
				//> Can never happen for self
				if(member.getHasLeft())
				{
					matchedMember.setAsLeft();
					m_oLogger.Info(new String("IMPORTANT : " + matchedMember.GetUniqueId() + " has left"));
					matchedMember.ResetLocalTime(GetMyLocalTime());
				}
				if(!matchedMember.HasLeft() 
						&& member.getHeartbeatCounter() > matchedMember.GetHeartbeatCounter())
				{
					matchedMember.ResetHeartbeatCounter(member.getHeartbeatCounter());
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
				if(!member.getHasLeft())
				{	
					m_oLogger.Info("Adding node to memberlist " + member.getIP() );
					String IP = member.getIP();
					int heartbeatCounter = member.getHeartbeatCounter();
					long localTime = GetMyLocalTime(); //Our machine localTime
					String uniqueId = member.getUniqueId();
					AddMemberToStruct(uniqueId, IP, heartbeatCounter, localTime);
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
		return MemberList.parseFrom(buffer);   //Need to make sure the message is the correct return type
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
						m_oLockW.lock();
						m_oLogger.Info(new String("IMPORTANT : Removing node : " + memberStruct.GetIP())); //UniqueId instead?
						iterator.remove();
						m_oLockW.unlock();
					}
					else
					{
						if((GetMyLocalTime() - memberStruct.GetLocalTime()) > m_nTfail)
						{
							m_oLogger.Info(new String("IMPORTANT : Suspected node : " + memberStruct.GetIP()));
							memberStruct.setAsSuspect();
						}
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
					m_oWriter.print(ipCSV.substring(1, ipCSV.length() - 1));
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
}
