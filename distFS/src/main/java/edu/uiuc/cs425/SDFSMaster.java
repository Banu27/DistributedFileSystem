package edu.uiuc.cs425;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.uiuc.cs425.FileMsg.FileReport;

public class SDFSMaster {
	
	public void MergeReport(byte[] reportBuf) throws InvalidProtocolBufferException
	{
		FileReport report = FileReport.parseFrom(reportBuf);
		// TODO complete impl of the actual merge with the MAsterBlockLocTable
		// get the nodeID and Block info from the below calls
		//report.getNodeID();
		//report.getSBlockIDsList()
	}
}
