package edu.uiuc.cs425;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SDFSFile {
	public int 			m_nSize;
	public String 		m_sFileID;
	public String 		m_sFilePath;
	
	public SDFSFile(int size, String sID, String path)
	{
		m_nSize 			= size;
		m_sFileID        	= sID;
		m_sFilePath       	= path;
	}
	
	public int AddFileData(ByteBuffer data)
	{
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(m_sFilePath);
			WritableByteChannel channel = Channels.newChannel(fos);
			channel.write(data);
			channel.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Commons.FAILURE;
		}
		return Commons.SUCCESS;
	}
	
	public ByteBuffer GetBuffer() throws IOException
	{
		Path path = Paths.get(m_sFilePath);
		byte[] data = Files.readAllBytes(path);
		ByteBuffer buf = ByteBuffer.wrap(data);
		return buf;
	}
}
