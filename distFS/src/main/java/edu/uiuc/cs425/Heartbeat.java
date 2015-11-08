package edu.uiuc.cs425;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

public class Heartbeat implements Runnable {
	private Membership 				m_oMship;
	private ConfigAccessor			m_oConfig;
	private	int						m_nGossipNodes;
	private int 					m_nGossipInterval;
	private int 					m_nHBSendPort;
	private int 					m_nHBCount;
	private boolean					m_bHB;
	private Logger					m_oLogger;
	private int						m_nLossRate;
	private Random 					m_oRands; 
	
	public int Initialize(Membership oMem, ConfigAccessor oConfig, Logger oLogger)
	{
		m_oLogger 				= oLogger;
		m_oMship 				= oMem;
		m_oConfig			 	= oConfig;
		m_nHBCount 				= 0;
		m_nGossipNodes			= oConfig.GossipNodes();
		m_nGossipInterval		= oConfig.HeartBeatInterval();
		m_nHBSendPort			= oConfig.HeartBeatPort();
		m_nLossRate 			= oConfig.LossRate();
		m_oRands 				= new Random();
		
		m_oLogger.Info("Initialized HeartBeat: GossipNodes=" + String.valueOf(m_nGossipNodes)
					+ " GossipInterval=" + String.valueOf(m_nGossipInterval) +
					" m_nHBSendPort=" + String.valueOf(m_nHBSendPort));
		m_bHB      				= true;
		
		return Commons.SUCCESS;
	}
	
	public void StopHB()
	{
		m_bHB = false;
	}
	
	public void DoHB()
	{
		ArrayList<String> vUniqueIds = m_oMship.GetMemberIds();
		String myID = m_oMship.UniqueId();
		vUniqueIds.remove(myID);
		int size = vUniqueIds.size();
		int currGossip = m_nGossipNodes;
		if(size < m_nGossipNodes) currGossip = size;
				
		Set<Integer> rands = Commons.RandomK(currGossip,size,m_oMship.GetMyLocalTime());
		// hack. always ask for k+ 1 and remove self or someother node
		m_oLogger.Info("Heartbeat count: " + String.valueOf(++m_nHBCount));
		for (Integer i : rands)
		{
			String ip = m_oMship.GetIP(vUniqueIds.get(i));
			HeartBeatProxy proxy = new HeartBeatProxy();
			if( proxy.Initialize(ip,m_nHBSendPort,m_oLogger) == Commons.SUCCESS )
			{
				try {
					proxy.SendMembershipList(m_oMship.GetMemberList());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				} // continue after exception
			}
			
		}
	}
	
	public void SendHB(String [] sIPs)
	{
		byte[] buf;
		String leaderId;
		try {
			buf = m_oMship.GetMemberList(); //Does this contain only 1 member - introducer??
		} catch (Exception e1) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e1));
			return;
		}
		for (String sIP: sIPs) {  
			HeartBeatProxy proxy = new HeartBeatProxy();
			if( proxy.Initialize(sIP,m_nHBSendPort,m_oLogger) == Commons.SUCCESS )
			{
				try {
					proxy.SendMembershipList(buf);
				} catch (IOException e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				} catch (Exception e) {
					m_oLogger.Error(m_oLogger.StackTraceToString(e));
				} // continue after exception
			}
		}
	}
	
	private void SendHBs()
	{
		try {
			Thread.sleep(m_nGossipInterval); 
		} catch (InterruptedException e) {
			m_oLogger.Error(m_oLogger.StackTraceToString(e));
			return;
		}
		while(m_bHB)
		{
			long start_time = System.nanoTime();
			m_oMship.IncrementHeartbeat();
			try {
				int randNum = m_oRands.nextInt(100);
				if(randNum > m_nLossRate)
					DoHB();
			} 
			// only catch the exception. There could be nodes that 
			// fail and will not connect. That is alright. Catch 
			// and continue
			catch (Exception e1) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e1));
				//return;
			}
			long diff = (System.nanoTime() - start_time)/1000000;
			try {
				Thread.sleep(m_nGossipInterval - diff);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				m_oLogger.Error(m_oLogger.StackTraceToString(e));
				return;
			}
		}
	}
	
	
	public void run() {
		SendHBs();
	}

}
