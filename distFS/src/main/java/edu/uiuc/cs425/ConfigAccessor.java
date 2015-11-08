package edu.uiuc.cs425;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import java.io.File;
import java.io.IOException;


/*
 <MembershipConfig>
   <Introducer nodeID="0" ip="10.16.8.85" port="9090" />
   <Heartbeat port="8192" interval="2000" gossipNodes="3" /> 
   <Failure interval="3000" checkPoint="" />
   <Logger path="" />
   <FileReport port="3425" interval="10000" />
   <Replication interval="30000" factor="3" />
   <SDFS dataDir="" clientSrcDir="" clientDestDir="" />
</MembershipConfig>
 */

public class ConfigAccessor {
	private String 		m_sIntroducerIP;
	private int 		m_nIntroducerPort;
	private int         m_nHeartBeatInterval;
	private int         m_nGossipNodes;
	private int         m_nHeartBeatPort;
	private int 		m_nFailureInterval;
	private String      m_sLogPath;
	private String      m_sCPPath;
	private int			m_nLossRate;
	private int         m_nFRPort;
	private int 		m_nFRInterval;
	private int			m_nReplicationFactor;;
	private int 		m_nReplicationCheckInterval;
	private String 		m_sSDFSDataDir;
	private String      m_sClientSrcDir;
	private String      m_sClientDestDir;
	
	public ConfigAccessor()
	{
		
	}
	
	public String SDFSDataDir()
	{
		return m_sSDFSDataDir;
	}
	
	public String ClientSrcDir()
	{
		return m_sClientSrcDir;
	}
	
	public String ClientDestDir()
	{
		return m_sClientDestDir;
	}
	
	public String       LogPath()
	{
		return m_sLogPath;
	}
	
	public String 		IntroducerIP()
	{
		return m_sIntroducerIP;
	}
	
	public int 		CmdPort()
	{
		return m_nIntroducerPort;
	}
	
	public int 		LossRate()
	{
		return m_nLossRate;
	}
	
	public int         HeartBeatInterval()
	{
		return m_nHeartBeatInterval;
	}
	
	public int         GossipNodes()
	{
		return m_nGossipNodes;
	}
	
	public int         HeartBeatPort()
	{
		return m_nHeartBeatPort;
	}
	
	public int 		FailureInterval()
	{
		return m_nFailureInterval;
	}
	
	public String GetCPPath()
	{
		return m_sCPPath;
	}
	
	public int GetFRPort()
	{
		return m_nFRPort;
	}
	
	public int GetFRInterval()
	{
		return m_nFRInterval;
	}
	
	public int GetReplicationFactor()
	{
		return m_nReplicationFactor;
	}
	
	public int GetReplicationCheckInterval()
	{
		return m_nReplicationCheckInterval;
	}
	
	public int Initialize(String sXMLFilePath)
	{
		File fXmlFile = new File(sXMLFilePath);
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		}
		Document doc;
		try {
			doc = dBuilder.parse(fXmlFile);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		}
				
		Node root = doc.getDocumentElement();
		NodeList nList = root.getChildNodes();

		for (int temp = 0; temp < nList.getLength(); temp++) {
			Node nNode = nList.item(temp);
			if (nNode.getNodeName() == "Introducer") {

				Element eElement = (Element) nNode;
				m_sIntroducerIP  = eElement.getAttribute("ip");
				m_nIntroducerPort = Integer.parseInt(eElement.getAttribute("port"));

			} else if(nNode.getNodeName() == "Heartbeat")
			{
				Element eElement = (Element) nNode;
				m_nHeartBeatPort  = Integer.parseInt(eElement.getAttribute("port"));
				m_nHeartBeatInterval = Integer.parseInt(eElement.getAttribute("interval"));
				m_nGossipNodes = Integer.parseInt(eElement.getAttribute("gossipNodes"));
				
			} else if(nNode.getNodeName() == "Failure")
			{
				Element eElement = (Element) nNode;
				m_nFailureInterval  = Integer.parseInt(eElement.getAttribute("interval"));
				m_sCPPath			= eElement.getAttribute("checkPoint");
			}
			else if(nNode.getNodeName() == "Loss")
			{
				Element eElement = (Element) nNode;
				m_nLossRate = Integer.parseInt(eElement.getAttribute("rate"));
			}
			else if(nNode.getNodeName() == "Logger")
			{
				Element eElement = (Element) nNode;
				m_sLogPath  = eElement.getAttribute("path");
			}
			else if (nNode.getNodeName() == "FileReport")
			{
				Element eElement = (Element) nNode;
				m_nFRPort  = Integer.parseInt(eElement.getAttribute("port"));
				m_nFRInterval = Integer.parseInt(eElement.getAttribute("interval"));
			}
			else if (nNode.getNodeName() == "Replication")
			{
				Element eElement = (Element) nNode;
				m_nReplicationFactor = Integer.parseInt(eElement.getAttribute("factor"));
				m_nReplicationCheckInterval = Integer.parseInt(eElement.getAttribute("interval"));
			}
			else if (nNode.getNodeName() == "SDFS")
			{
				Element eElement = (Element) nNode;
				
				m_sSDFSDataDir		= eElement.getAttribute("dataDir");
				m_sClientSrcDir 	= eElement.getAttribute("clientSrcDir");
				m_sClientDestDir 	= eElement.getAttribute("clientDestDir");
			}
		}
		return Commons.SUCCESS;
	}
	

}

