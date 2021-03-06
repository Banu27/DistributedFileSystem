package edu.uiuc.cs425;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.thrift.TException;

import java.util.Map.Entry;

public class Election {

	private enum 			m_enumState { PROGRESS, LEADERALIVE};
	private m_enumState		m_eState;
	private Membership 		m_oMembershipList;
	private int				m_nUniqueSerialNumber;
	private Logger			m_oLogger;
	private int				m_nSentElectionMessages;
	private String			m_sLeaderId;
	private int				m_nServicePortForProxys;
	private SDFSMaster		m_oSDFSMaster;	
	
	
	
	
	public void SetSerialNumber(int serialNumber)
	{
		m_nUniqueSerialNumber = serialNumber;
		m_oLogger.Info(new String("Unique serial number set as : " + String.valueOf(m_nUniqueSerialNumber)));
	}
	
	public void SetLeader(String leaderId)
	{
		m_sLeaderId = leaderId;
		m_eState = m_enumState.LEADERALIVE;
	}
	
	public void SetNewLeader(String leaderId)
	{
		m_sLeaderId = leaderId;
		//MASTER HAS TO FINISH		
		if(Commons.SUCCESS == m_oSDFSMaster.MasterSetup())
			m_eState = m_enumState.LEADERALIVE;
	}
		
	public String GetLeaderId()
	{
		return m_sLeaderId;
	}
	
	public String GetLeaderIP()
	{
		return m_oMembershipList.GetIP(m_sLeaderId);
	}
	
	public void Initialize(Membership memberObject, Logger loggerObject, int servicePort, SDFSMaster master) //References from controller
	{
		//Initialize m_oMembershipList
		m_oMembershipList = memberObject;
		m_oLogger = loggerObject;
		m_nServicePortForProxys = servicePort;
		m_oSDFSMaster = master;
	}
	
	public boolean IsLeaderAlive()
	{
		if (m_eState == m_enumState.LEADERALIVE)
			return true;
		return false;
	}
	
	public void StartElection()
	{
		m_nSentElectionMessages = 0;
		m_eState = m_enumState.PROGRESS;
		m_sLeaderId = null;
		m_oLogger.Info(new String("Starting Election now !"));
		m_oLogger.Info(new String("My serial number : " + String.valueOf(m_nUniqueSerialNumber)));
		SendElectionMessages();
	}
	
	public void SendElectionMessages()
	{
		HashMap<Integer,String> SnoListAndIPList = m_oMembershipList.GetSNoListAndIPList();
		
		Set<Entry<Integer, String>> set = SnoListAndIPList.entrySet();
		Iterator<Entry<Integer,String>> iterator = set.iterator();
		while(iterator.hasNext()) {
	        Map.Entry mentry = (Map.Entry)iterator.next(); 
			int Sno = (Integer) mentry.getKey();
			m_oLogger.Info(new String("Checking Sno : " + String.valueOf(Sno)));
			if( Sno < m_nUniqueSerialNumber)
			{
				CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
				m_oLogger.Info("Trying to initiate connection with " + mentry.getValue().toString() + " at " + String.valueOf(m_nServicePortForProxys));
				if(Commons.SUCCESS == ProxyTemp.Initialize(mentry.getValue().toString(),m_nServicePortForProxys,m_oLogger))
				{	try {
							ProxyTemp.ReceiveElectionMessage(); //if this throws exception, the next line
							m_nSentElectionMessages ++;
					} catch (TException e) {
						m_oLogger.Error("Failed to send election message to " + mentry.getValue().toString());
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
						
					}
				}
			}
		}
		if(m_nSentElectionMessages == 0)
		{
			m_oLogger.Info(new String("No election messages were sent, I AM THE LEADER"));
			SetNewLeader(m_oMembershipList.UniqueId());
			m_oLogger.Info(new String("My leader id is : " + m_sLeaderId));
			SendCoordinationMessage();
		}
	}
	
	public void SendCoordinationMessage()
	{
		ArrayList<String> IPList = m_oMembershipList.GetMemberIds();
		m_oLogger.Info("Sending coordination messages");
		Iterator<String> it = IPList.iterator();
		while(it.hasNext())
		{
			CommandIfaceProxy ProxyTemp = new CommandIfaceProxy();
			String sIP = m_oMembershipList.GetIP(it.next().toString());
			if(Commons.SUCCESS == ProxyTemp.Initialize(sIP,m_nServicePortForProxys,m_oLogger))
			{	
				m_oLogger.Info("sending message to " + sIP);
				try {
						ProxyTemp.ReceiveCoordinationMessage(m_sLeaderId);
					} catch (TException e) {
						m_oLogger.Error(m_oLogger.StackTraceToString(e));
					}
			}

		}
		
	}
	
	public void ReceiveCoordinationMessage(String leaderId)
	{
		m_oLogger.Info("Received cordination from " + leaderId);
		SetLeader(leaderId);
	}
		
	public void ReceiveElectionMessage()
	{
		StartElection();
	}
}
