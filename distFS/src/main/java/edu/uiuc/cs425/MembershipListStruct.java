package edu.uiuc.cs425;

public class MembershipListStruct {

	private enum 			m_enumState { ALIVE, SUSPECT, LEFT };
	private String 			m_sUniqueId;
	private String 			m_sIP;
	private int 			m_nHeatbeatCounter;
	private long 			m_nLocalTime;
	private m_enumState 	m_eState;
	
	public MembershipListStruct(String IP, String uniqueId, int heartbeatCounter, long localTime)
	{
		m_sUniqueId = uniqueId;
		m_sIP = IP;
		m_nHeatbeatCounter = heartbeatCounter;
		m_nLocalTime = localTime;
		m_eState = m_enumState.ALIVE;
	}
	
	public void Print()
	{
		System.out.println(m_sUniqueId + " | " + String.valueOf(m_nHeatbeatCounter) + " | " +String.valueOf(m_nLocalTime)
					+ " | " + String.valueOf(m_eState));
	}
	
	public String GetStr()
	{
		String str = m_sUniqueId + " " + String.valueOf(m_nHeatbeatCounter) + " " +String.valueOf(m_nLocalTime)
		+ " " + String.valueOf(m_eState) + "\n";
		return str;
	}
	
	public void ResetLocalTime(long localTime)
	{
		m_nLocalTime = localTime;
	}
	
	public void ResetHeartbeatCounter(int heartbeatCounter)
	{
		m_nHeatbeatCounter = heartbeatCounter;
	}
	
	public int GetHeartbeatCounter()
	{
		return m_nHeatbeatCounter;
	}
	
	public String GetUniqueId()
	{
		return m_sUniqueId;
	}
	
	public long GetLocalTime()
	{
		return m_nLocalTime;
	}
	
	public boolean IsSuspect()
	{
		return (m_eState == m_enumState.SUSPECT);
	}
		
	public void setAsSuspect()
	{
		m_eState = m_enumState.SUSPECT;
	}
	
	public String GetIP()
	{
		return m_sIP;
	}
	public boolean HasLeft()
	{
		return (m_eState == m_enumState.LEFT);
	}
	
	public void setAsLeft()
	{
		m_eState = m_enumState.LEFT;
	}
	
	public void setAsAlive()
	{
		m_eState = m_enumState.ALIVE;
	}
}
