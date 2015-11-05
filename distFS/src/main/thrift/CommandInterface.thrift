namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
   	int 					JoinGroup(); #Introducer
   	
   	string 					GetLeaderId(); #Introducer
   	string                  GetLeaderIP(); #Membership
   	binary 					GetMembershipList(); #Introducer
   	
   	oneway void 			ReceiveElectionMessage(); #Election
   	oneway void 			ReceiveCoordinationMessage(1:string leaderId); #Election
   	bool 					IsLeaderAlive(); #Election
	
	oneway void 			DeleteFile(1:string fileID); #Master
 	string 					RequestAddFile(1:string filename); #Master
 	set<string> 			GetFileLocations(1:string filename); #Master
	list<string> 			GetAvailableFiles() #Master   	
	void 					FileStorageAck(1:string filename, 2:string incomingID) #Master	
	
	binary 					GetFile(1:string filename) #NodeMgr
	int 					AddFile(1:int size, 2:string fileID, 3:binary payload, 4:bool replicate); #NodeFileManager
	set<string>				GetFileList(); #NodeMgr
 	
	
}
