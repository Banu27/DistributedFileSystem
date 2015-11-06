package edu.uiuc.cs425;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.uiuc.cs425.CommandInterface.Iface;
import edu.uiuc.cs425.CommandInterface;

public class CommandIfaceProxy implements Iface {
	
	private CommandInterface.Client m_oClient;
	private TTransport transport;
	private Logger m_oLogger;
	
	public CommandIfaceProxy()
	{
		m_oClient = null;
	}
	
	public int Initialize(String sIP,int nPort,Logger oLogger)
	{
		m_oLogger	= oLogger;
		transport = new TFramedTransport(new TSocket(sIP, nPort));
	    try {
			transport.open();
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			m_oLogger.Error(new String("Failed to initialize MemberIntro proxy")); //IP????
			return Commons.FAILURE;
		}
	    m_oClient = new CommandInterface.Client(new TBinaryProtocol(transport));
	    m_oLogger.Info(new String("Created Member Proxy"));
		return Commons.SUCCESS;
	}

	public int JoinGroup() throws TException {
		// TODO Auto-generated method stub
		m_oLogger.Info(new String("Joining Group"));
		return m_oClient.JoinGroup();
	}
	
	public String GetLeaderId() throws TException{
		return m_oClient.GetLeaderId();
	}

	public ByteBuffer GetMembershipList() throws TException {
		// TODO Auto-generated method stub
		m_oLogger.Info(new String("Receiving MembershipList"));
		return m_oClient.GetMembershipList();
	}

	public void Close()
	{
		transport.close();
	}
	
	public void ReceiveElectionMessage() throws TException {
		 m_oClient.ReceiveElectionMessage();	
	}
	public boolean IsLeaderAlive() throws TException {
		return m_oClient.IsLeaderAlive();
	}
	
	public void ReceiveCoordinationMessage(String leaderId) throws TException {
		
		m_oClient.ReceiveCoordinationMessage(leaderId);
	}

	public Set<String> GetFileLocations(String Filename) throws TException
	{
		return m_oClient.GetFileLocations(Filename);
	}

	public String RequestAddFile(String filename) throws TException
	{
		return m_oClient.RequestAddFile(filename);
	}
	
	public int AddFile(int size, String fileID, ByteBuffer payload, boolean replicate) throws TException {
		return m_oClient.AddFile(size, fileID, payload, replicate);
	}

	public void DeleteFile(String fileID) throws TException {
		m_oClient.DeleteFile(fileID);
	}
	
	public void FileStorageAck(String filename, String incomingID) throws TException
	{
		m_oClient.FileStorageAck(filename,incomingID);
	}
	
	public List<String> GetAvailableFiles() throws TException
	{
		return m_oClient.GetAvailableFiles();
	}

	public String GetLeaderIP() throws TException {
		return m_oClient.GetLeaderIP();
	}

	public ByteBuffer GetFile(String filename) throws TException
	{
		return m_oClient.GetFile(filename);
	}


	public Set<String> GetFileList() throws TException {
		return m_oClient.GetFileList();
	}

	public Set<String> RequestFileReport(String receiverIp) throws TException
	{
		return m_oClient.RequestFileReport(receiverIp);
	}

	public void RequestFileCopy(String filename, String nodeID) throws TException
	{
		m_oClient.RequestFileCopy(filename,nodeID);
	}
	
	
}
