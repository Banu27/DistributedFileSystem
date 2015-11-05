package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;

import edu.uiuc.cs425.CommandInterface.Iface;

public class CommandIfaceImpl implements Iface {

	private Introducer 		m_oIntroObj;
	private Election  		m_oElection;
	private NodeFileMgr     m_oNodeMgr;
	private SDFSMaster		m_oSDFSMaster;
	
	public void SetIntoObj(Introducer oIntroObj)
	{
		this.m_oIntroObj = oIntroObj;
	}
	
	public void SetNMObj(NodeFileMgr oNMObj)
	{
		this.m_oNodeMgr = oNMObj;
	}
	
	public void setElectionObj(Election oElectionObj)
	{
		this.m_oElection = oElectionObj;
	}
	
	public void SetMasterObject(SDFSMaster oMaster)
	{
		this.m_oSDFSMaster = oMaster;
	}
	
	// not sure the purpose of this function but might be needed in the future
	public int Initialize()
	{
		return Commons.SUCCESS;
	}
	
	public int JoinGroup() throws TException {
		return m_oIntroObj.JoinGroup();
	}

	public String GetLeaderId()
	{
		return m_oElection.GetLeaderId();
	}
	
	public int IsLeaderAlive()
	{
		return m_oElection.IsLeaderAlive();
	}
	
	
	public ByteBuffer GetMembershipList() throws TException {
		return m_oIntroObj.GetMembershipList();
	}
	
	public void ReceiveElectionMessage() throws TException {
		
		m_oElection.ReceiveElectionMessage();		
	}
	
	public void ReceiveCoordinationMessage(String leaderId) throws TException {
		m_oElection.ReceiveCoordinationMessage(leaderId);
	}
	
	public Set<String> GetFileLocations(String filename) throws TException {
		return m_oSDFSMaster.GetFileLocations(filename);
	}
	
	//At NodeManager
	public void GetFile(String filename) throws TException
	{
		//Write code to copy file to the client
	}
	
	//At NodeManager
	public int AddFile(int size, String fileID, ByteBuffer payload, boolean replicate) throws TException {
				
		// create the SDFS file obj
		SDFSFile file_ = new SDFSFile(size, fileID, Commons.SDFS_LOC + fileID);
		file_.AddFileData(payload);
		// forward the info to the node manager
		return m_oNodeMgr.AddFile(file_, replicate);
		
		//ACK??
		
	}
	
	public String RequestAddFile(String filename) throws TException
	{
		return m_oSDFSMaster.RequestAddFile(filename);
	}
	
	
	public List<String> GetAvailableFiles() throws TException
	{
		return m_oSDFSMaster.GetAvailableFiles();
	}

	
	public void FileStorageAck(String filename, String incomingID) throws TException
	{
		m_oSDFSMaster.FileStorageAck(filename, incomingID);
	}


	public void DeleteFile(String fileID) throws TException {
		m_oNodeMgr.DeleteFile(fileID);	
	}

}
