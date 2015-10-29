package edu.uiuc.cs425;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

public class Election {

	private Membership 		m_oMembershipList;
	private int				m_nCurrentSerialNumber;
	
	private boolean isMasterAlive()
	{
		return true;
	}
	
	private void Intialize()
	{
		//initalize m_oMembershipList
		m_oMembershipList = new Membership();
		m_nCurrentSerialNumber = m_oMembershipList.GetUniqueSerialNumber();		
	}
	
	public void sendElectionMessages()
	{
		Vector<Integer> SnoList = m_oMembershipList.GetSNoList();
		Vector<String> IPList = m_oMembershipList.GetIPList();
		
		Iterator it = SnoList.iterator();
		while(it.hasNext())
		{
			if( (Integer) it.next() < m_nCurrentSerialNumber)
			{
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
