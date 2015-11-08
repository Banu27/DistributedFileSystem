package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;

import edu.uiuc.cs425.CommandInterface.Iface;

public class CommandIfaceImpl implements Iface {

	private Introducer 		m_oIntroObj;
	private Election  		m_oElection;
	private NodeFileMgr     m_oNodeMgr;
	private SDFSMaster		m_oSDFSMaster;
	private ConfigAccessor  m_oConfig;
	
	
	public int Initialize(Introducer oIntroObj, NodeFileMgr oNMObj, Election oElectionObj, 
			SDFSMaster oMaster, ConfigAccessor oAccessor)
	{
		this.m_oIntroObj = oIntroObj;
		this.m_oNodeMgr = oNMObj;
		this.m_oElection = oElectionObj;
		this.m_oSDFSMaster = oMaster;
		this.m_oConfig = oAccessor;
		return Commons.SUCCESS;
	}
	
	public int JoinGroup() throws TException {
		return m_oIntroObj.JoinGroup();
	}

	public String GetLeaderId()
	{
		return m_oElection.GetLeaderId();
	}
	
	public String GetLeaderIP()
	{
		return m_oElection.GetLeaderIP();
	}
	
	public boolean IsLeaderAlive()
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
		Set<String> ret = m_oSDFSMaster.GetFileLocations(filename);
		if( ret == null) 
			ret = new HashSet<String>();
		return ret;
	}
	
	
	
	//At NodeManager
	public int AddFile(int size, String fileID, ByteBuffer payload, boolean replicate) throws TException {
				
		// create the SDFS file obj
		SDFSFile file_ = new SDFSFile(size, fileID, m_oConfig.SDFSDataDir() + fileID);
		file_.AddFileData(payload);
		// forward the info to the node manager
		return m_oNodeMgr.AddFile(file_, replicate);
		
	}
	
	
	
	public String RequestAddFile(String filename) throws TException
	{
		return m_oSDFSMaster.RequestAddFile(filename);
	}
	
	
	public List<String> GetAvailableFiles() throws TException
	{
		List<String> ret = m_oSDFSMaster.GetAvailableFiles();
		if( ret == null)
		{
			ret = new ArrayList<String>();
		}
		return ret;
	}

	
	public void FileStorageAck(String filename, String incomingID) throws TException
	{
		m_oSDFSMaster.FileStorageAck(filename, incomingID);
	}


	public void DeleteFile(String fileID) throws TException {
		m_oNodeMgr.DeleteFile(fileID);	
	}

	public ByteBuffer GetFile(String filename) throws TException {
		return m_oNodeMgr.GetFile(filename);
	}

	public Set<String> GetFileList() throws TException {
		Set<String> ret = m_oNodeMgr.GetFileList();
		if( ret == null) 
			ret = new HashSet<String>();
		return ret;
	}

	public void RequestFileCopy(String filename, String nodeID) throws TException
	{
		//How do I get file size????
		//How do I get payload???
		//SDFSFile file_ = new SDFSFile(size, filename, Commons.SDFS_LOC + filename);
		//file_.AddFileData(payload);
		m_oNodeMgr.RequestFileCopy(filename,nodeID);
	}

}
