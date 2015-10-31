package edu.uiuc.cs425;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

public class Election {

	private Membership 		m_oMembershipList;
	private int				m_nCurrentSerialNumber;
	private Logger			m_oLogger;
	private int				m_nSentElectionMessages;
	private String			m_sLeaderId;
	private String 			m_sUniqueId;
	
	public boolean IsMasterAlive()
	{
		return true;
	}
	
	public void SetLeader(String leaderId)
	{
		m_sLeaderId = leaderId;
	}
	
	
	public void Initialize(Membership memberObject, Logger loggerObject) //References from controller
	{
		//Initialize m_oMembershipList
		m_oMembershipList = memberObject;
		m_nCurrentSerialNumber = m_oMembershipList.GetUniqueSerialNumber();		
		m_oLogger = loggerObject;
		m_sUniqueId = m_oMembershipList.UniqueId();
		m_sLeaderId = new String();
	}
	
	public void StartElection()
	{
		m_nSentElectionMessages = 0;
		SendElectionMessages();
	}
	
	public void SendElectionMessages()
	{
		Vector<Integer> SnoList = m_oMembershipList.GetSNoList();
		Vector<String> IPList = m_oMembershipList.GetIPList();
		
		Iterator itSno = SnoList.iterator();
		Iterator itIP = IPList.iterator();
		while(itSno.hasNext() && itIP.hasNext())
		{
			if( (Integer) itSno.next() < m_nCurrentSerialNumber)
			{
				//CommServerTemp = new Proxy??(//Use the IP here)??
				//if connection not success exception caught - ex leader case
				//No timeouts
				//if(Commons.Success  = CommServerTemp.receiveElectionMessage())
					m_nSentElectionMessages ++;
				//itIP.next().toString() - This is the IP. 
				//Create a connection and send the Election message
				//proxy.sendElectionMessages();
			}
			if(m_nSentElectionMessages == 0)
			{
				SendCoordinationMessage();
			}
		}
	}
	
	public void SendCoordinationMessage()
	{
		Vector<String> IPList = m_oMembershipList.GetIPList();
		
		Iterator<String> it = IPList.iterator();
		while(it.hasNext())
		{
			 String IP = it.next().toString();
			{
				//Create a connection and send Coordination Message
				//proxy.receiveCoordinationMessage(m_sUniqueId);
			}
		}
	}
	
	public void ReceiveCoordinationMessage(String leaderId)
	{
		m_sLeaderId = leaderId;
	}
		
	public int ReceiveElectionMessage()
	{
		SendElectionMessages();
		return Commons.SUCCESS;
	}
}
