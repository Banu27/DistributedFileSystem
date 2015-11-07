package edu.uiuc.cs425;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.uiuc.cs425.CommandInterface.Iface;
import java.util.concurrent.atomic.AtomicInteger;

public class Introducer { 

	private Membership 		m_oMembershipObject; //The membership object of the introducer
	private Logger 			m_oLogger;
	private int				m_nSerialNumber;
	private Election		m_oElection;
	private PrintWriter		m_oWriter;
	private ConfigAccessor  m_oAccesor;
	
	public Introducer()
	{
	}
		
	public int Initialize(Membership member,Logger oLogger, Election election, ConfigAccessor oAccesor)
	{
		m_oLogger 					= oLogger;
		m_oMembershipObject 		= member;
		m_nSerialNumber 			= 1;
		m_oLogger.Info(new String("Introducer is up"));
		m_oElection 				= election;
		m_oAccesor					= oAccesor;
		return Commons.SUCCESS;
	}
	
	public void setSno(int s_no)
	{
		m_nSerialNumber = s_no;
	}
	
	public String GetLeaderId() 
	{
		return m_oElection.GetLeaderId();
	}
	
	public int JoinGroup() throws TException {
		m_oLogger.Info(new String("New node has joined with serial number : " + String.valueOf(m_nSerialNumber)));
		
		
		int newSerialNo = m_nSerialNumber;
		m_nSerialNumber++;
		// checkpoint the current serial number
		try {
			m_oWriter = new PrintWriter(m_oAccesor.GetCPPath() + "_SNO", "UTF-8");
		} catch (FileNotFoundException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		} catch (UnsupportedEncodingException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
		}
		m_oWriter.print(m_nSerialNumber);
	 	m_oWriter.close();	
		return newSerialNo;//Commons.SUCCESS;
	}

	public ByteBuffer GetMembershipList() throws TException {
		// TODO Auto-generated method stub
		try {
			return ByteBuffer.wrap(m_oMembershipObject.GetMemberList());
		} catch (Exception e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));	
			throw new TException();
		}
		
	}

}
