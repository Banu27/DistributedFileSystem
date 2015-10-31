package edu.uiuc.cs425;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.uiuc.cs425.CommandInterface.Iface;

public class CommandIfaceImpl implements Iface {

	private Introducer 		m_oIntroObj;
	private Election  		m_oElection;
	
	public void SetIntoObj(Introducer oIntroObj)
	{
		this.m_oIntroObj = oIntroObj;
	}
	
	public void setElectionObj(Election oElectionObj)
	{
		this.m_oElection = oElectionObj;
	}
	
	// not sure the purpose of this function but might be needed in the future
	public int Initialize()
	{
		return Commons.SUCCESS;
	}
	
	public int JoinGroup() throws TException {
		// TODO Auto-generated method stub
		return m_oIntroObj.JoinGroup();
	}

	public String GetLeaderId()
	{
		return m_oIntroObj.GetLeaderId();
	}
	
	public ByteBuffer GetMembershipList() throws TException {
		// TODO Auto-generated method stub
		return m_oIntroObj.GetMembershipList();
	}
	
	public int ReceiveElectionMessage() throws TException {
		
		return m_oElection.ReceiveElectionMessage();		
	}
	
	public void ReceiveCoordinationMessage(String leaderId) throws TException {
		m_oElection.ReceiveCoordinationMessage(leaderId);
	}
	

	public void ReceiveCoordinationMessage() throws TException {
		// TODO Auto-generated method stub
		
	}

}
