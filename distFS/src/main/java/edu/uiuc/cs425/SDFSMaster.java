package edu.uiuc.cs425;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.uiuc.cs425.BlockMsg.BlockReport;

public class SDFSMaster {
	
	public void MergeReport(byte[] reportBuf) throws InvalidProtocolBufferException
	{
		BlockReport report = BlockReport.parseFrom(reportBuf);
		// TODO complete impl of the actual merge with the MAsterBlockLocTable
		// get the nodeID and Block info from the below calls
		//report.getNodeID();
		//report.getSBlockIDsList()
	}
}
