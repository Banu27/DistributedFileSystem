package edu.uiuc.cs425;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.uiuc.cs425.CommandInterface.Iface;
import java.util.concurrent.atomic.AtomicInteger;

public class Introducer { 

	private Membership 		m_oMembershipObject; //The membership object of the introducer
	private Logger 			m_oLogger;
	private int				m_nSerialNumber;
	private Election		m_oElection;
	
	public Introducer(Membership member,Logger oLogger, Election election)
	{
		m_oLogger = oLogger;
		m_oMembershipObject = member;
		m_nSerialNumber = 1;
		m_oLogger.Info(new String("Introducer is up"));
		m_oElection = election;
	}
		
	public String GetLeaderId() 
	{
		return m_oElection.GetLeaderId();
	}
	
	public int JoinGroup() throws TException {
		m_oLogger.Info(new String("New node has joined with serial number : " + String.valueOf(m_nSerialNumber)));
		return m_nSerialNumber++;//Commons.SUCCESS;
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
