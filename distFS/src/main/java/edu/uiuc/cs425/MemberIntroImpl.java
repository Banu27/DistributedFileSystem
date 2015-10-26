package edu.uiuc.cs425;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import edu.uiuc.cs425.MemberIntroducer.Iface;

public class MemberIntroImpl implements Iface {

	private Introducer m_oIntroObj;
	
	public void SetIntoObj(Introducer oIntroObj)
	{
		this.m_oIntroObj = oIntroObj;
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

	public ByteBuffer GetMembershipList() throws TException {
		// TODO Auto-generated method stub
		return m_oIntroObj.GetMembershipList();
	}

}
