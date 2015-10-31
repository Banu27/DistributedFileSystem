package edu.uiuc.cs425;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.uiuc.cs425.MemberIntroducer.Iface;
import java.util.concurrent.atomic.AtomicInteger;

public class Introducer { //Why implements Iface??

	private Membership 		m_oMembershipObject; //The membership object of the introducer
	private Logger 			m_oLogger;
	private int				m_nSerialNumber;
	private String			m_sLeaderUniqueId;
	
	public Introducer(Membership member,Logger oLogger)
	{
		m_oLogger = oLogger;
		m_oMembershipObject = member;
		m_nSerialNumber = 1;
		m_oLogger.Info(new String("Introducer is up"));
	}
	
	public String GetLeaderId() 
	{
		return m_sLeaderUniqueId;
	}
	
	public int JoinGroup() throws TException {
		// TODO Auto-generated method stub
		//No threading so no lock
		synchronized (this) {
			m_nSerialNumber = m_nSerialNumber + 1;	
		}
		m_oLogger.Info(new String("New node has joined"));
		return m_nSerialNumber;//Commons.SUCCESS;
	}

	public ByteBuffer GetMembershipList() throws TException {
		// TODO Auto-generated method stub
		try {
			return ByteBuffer.wrap(m_oMembershipObject.GetMemberList());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));	
			throw new TException();
		}
		
	}

}
