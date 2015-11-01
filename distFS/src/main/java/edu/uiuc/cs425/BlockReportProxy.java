
package edu.uiuc.cs425;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class BlockReportProxy {
	
	private DatagramSocket  m_oSocket;
	private InetAddress     m_oHost;
	private int				m_Port;
	private Logger 			m_oLogger;
	
	public int Initialize(String ip, int port, Logger oLogger)
	{
		m_oLogger = oLogger;
		try {
			m_oSocket = new DatagramSocket();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		}
		 try {
			m_oHost       			= InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return Commons.FAILURE;
		}
		 m_Port = port;
		 return Commons.SUCCESS;
	}
	
	
	void SendBlockReport(byte[] report) throws IOException
	{
		DatagramPacket packet     = new DatagramPacket(report, report.length, m_oHost, m_Port);
		m_oSocket.send(packet);
	}
	void close()
	{
		m_oSocket.close();
	}


}
