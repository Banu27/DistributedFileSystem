package edu.uiuc.cs425;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Block {
	private int 		nSize;
	private String 		sBlockID;
	private String 		sFilePath;
	
	public Block(int size, String sID, String path)
	{
		nSize 			= size;
		sBlockID        = sID;
		sFilePath       = path;
	}
	
	public int AddBlockData(byte[] data)
	{
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(sFilePath);
			fos.write(data);
			fos.close();
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
	
	public byte[] GetBuffer() throws IOException
	{
		Path path = Paths.get(sFilePath);
		byte[] data = Files.readAllBytes(path);
		return data;
	}
}
