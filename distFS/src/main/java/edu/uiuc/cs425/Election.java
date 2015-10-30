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
	
	public boolean isMasterAlive()
	{
		return true;
	}
	
	public void Initialize(Membership memberObject, Logger loggerObject) //References from controller
	{
		//Initialize m_oMembershipList
		m_oMembershipList = memberObject;
		m_nCurrentSerialNumber = m_oMembershipList.GetUniqueSerialNumber();		
		m_oLogger = loggerObject;
	}
	
	public void sendElectionMessages()
	{
		Vector<Integer> SnoList = m_oMembershipList.GetSNoList();
		Vector<String> IPList = m_oMembershipList.GetIPList();
		
		Iterator itSno = SnoList.iterator();
		Iterator itIP = IPList.iterator();
		while(itSno.hasNext() && itIP.hasNext())
		{
			if( (Integer) itSno.next() < m_nCurrentSerialNumber)
			{
				//itIP.next().toString() - This is the IP. 
				//Create a connection and send the Election message
				//proxy.sendElectionMessages();
			}
		}
	}
	
	public void sendCoordinationMessage()
	{
		Vector<String> IPList = m_oMembershipList.GetIPList();
		
		Iterator it = IPList.iterator();
		while(it.hasNext())
		{
			 String IP = it.next().toString();
			{
				//Create a connection and send Coordination Message
				//proxy.sendCoordinationMessage();
			}
		}
	}
	
	public void receiveElectionMessage()
	{
		sendElectionMessages();
	}
}
