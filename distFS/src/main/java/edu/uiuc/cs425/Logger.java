package edu.uiuc.cs425;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
public class Logger {
	
	private PrintWriter m_oWriter;
	private Lock 		m_oLock;
	
	public Logger()
	{
		m_oWriter = null;
		m_oLock = new ReentrantLock();
	}
	
	
	public int Initialize(String sFile)
	{
		try {
			m_oWriter = new PrintWriter(sFile, "UTF-8");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		} 
		return Commons.SUCCESS;
	}
	
	public void Info(String str)
	{
		String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
		String concatStr = timeStamp + " INFO " + str;
		m_oLock.lock();
		m_oWriter.println(concatStr);
		m_oWriter.flush();
		m_oLock.unlock();
	}
	
	public void Debug(String str)
	{
		String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
		String concatStr = timeStamp + " DEBUG " + str;
		m_oLock.lock();
		m_oWriter.println(concatStr);
		m_oLock.unlock();
	}
	
	public void Warning(String str)
	{
		String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
		String concatStr = timeStamp + " WARNING " + str;
		m_oLock.lock();
		m_oWriter.println(concatStr);
		m_oWriter.flush();
		m_oLock.unlock();
	}
	
	public void Error(String str)
	{
		String timeStamp = new SimpleDateFormat("HH:mm:ss:SSS").format(Calendar.getInstance().getTime());
		String concatStr = timeStamp + " ERROR " + str;
		m_oLock.lock();
		m_oWriter.println(concatStr);
		m_oWriter.flush();
		m_oLock.unlock();
	}

	public String StackTraceToString(Exception ex)
	{	StringWriter errors = new StringWriter();
		ex.printStackTrace(new PrintWriter(errors));
		return errors.toString();
	}

	
	public void Close()
	{
		m_oWriter.close();
	}

}
