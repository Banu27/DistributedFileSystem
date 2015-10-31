package edu.uiuc.cs425;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.thrift.TException;

import java.util.Map.Entry;

public class Election {

	private Membership 		m_oMembershipList;
	private int				m_nCurrentSerialNumber;
	private Logger			m_oLogger;
	private int				m_nSentElectionMessages;
	private String			m_sLeaderId;
	private int				m_nServicePortForProxys;
	
	public boolean IsMasterAlive()
	{
		return true;
	}
	
	public void SetLeader(String leaderId)
	{
		m_sLeaderId = leaderId;
	}
		
	public String GetLeaderId()
	{
		return m_sLeaderId;
	}
	
	
	public void Initialize(Membership memberObject, Logger loggerObject, int servicePort) //References from controller
	{
		//Initialize m_oMembershipList
		m_oMembershipList = memberObject;
		m_nCurrentSerialNumber = m_oMembershipList.GetUniqueSerialNumber();		
		m_oLogger = loggerObject;
		//m_oLogger.Info(new String("My unique id is : " + m_sUniqueId));
		//m_sLeaderId = new String();
		m_nServicePortForProxys = servicePort;
	}
	
	public void StartElection()
	{
		m_nSentElectionMessages = 0;
		m_oLogger.Info(new String("Starting Election now !"));
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
			m_oLogger.Info(new String("Going through the list of IPs"));
			if( (Integer) itSno.next() < m_nCurrentSerialNumber)
			{
				CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
				if(Commons.SUCCESS == ProxyTemp.Initialize(itIP.next().toString(),m_nServicePortForProxys,m_oLogger))
				{	try {
						if(Commons.SUCCESS  == ProxyTemp.ReceiveElectionMessage())
							m_nSentElectionMessages ++;
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		if(m_nSentElectionMessages == 0)
		{
			m_oLogger.Info(new String("No election messages were sent, I AM THE LEADER"));
			m_sLeaderId = m_oMembershipList.UniqueId();
			m_oLogger.Info(new String("My leader id is : " + m_sLeaderId));
			SendCoordinationMessage();
		}
	}
	
	public void SendCoordinationMessage()
	{
		Vector<String> IPList = m_oMembershipList.GetIPList();
		
		Iterator<String> it = IPList.iterator();
		while(it.hasNext())
		{
			CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
			if(Commons.SUCCESS == ProxyTemp.Initialize(it.next().toString(),m_nServicePortForProxys,m_oLogger))
			{	
				try {
						ProxyTemp.ReceiveCoordinationMessage(m_sLeaderId);
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
