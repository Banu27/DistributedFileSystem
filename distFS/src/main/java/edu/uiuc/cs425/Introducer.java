package edu.uiuc.cs425;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.uiuc.cs425.MemberIntroducer.Iface;

public class Introducer implements Iface { //Why implements Iface??

	private Membership m_oMembershipObject; //The membership object of the introducer
	private Logger m_oLogger;
	
	public Introducer(Membership member,Logger oLogger)
	{
		m_oLogger = oLogger;
		m_oMembershipObject = member;
		m_oLogger.Info(new String("Introducer is up"));
	}
	
	public int JoinGroup() throws TException {
		// TODO Auto-generated method stub
		//No threading so no lock
		m_oLogger.Info(new String("New node has joined"));
		return Commons.SUCCESS;
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
