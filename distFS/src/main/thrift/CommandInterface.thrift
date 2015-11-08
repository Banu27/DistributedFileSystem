namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
  	int 			JoinGroup(); 													#NewNode to Introducer
   	string 			GetLeaderId(); 												#NewNode to Introducer
   	binary 			GetMembershipList(); 										#NewNode to Introducer
   	oneway void 	ReceiveElectionMessage(); 								#Node that detected Failure to lower id node, Election
   	oneway void 	ReceiveCoordinationMessage(1:string leaderId); 			#Leader to all other nodes, Election
   	bool 			IsLeaderAlive(); 												#NewNode to introducer NewIntroducer to any alive node, Election
	oneway void 	DeleteFile(1:string fileID); 							#Any client to Master
 	int 			AddFile(1:int size, 2:string fileID, 3:binary payload, 4:bool replicate); #Client to NodeFileManager
 	string 			RequestAddFile(1:string filename); 							#Client to Master
 	set<string> 	GetFileLocations(1:string filename); 					#Client to Master
	list<string> 	GetAvailableFiles(); 									#Client to Master   
	void  			DeleteFileMaster(1:string filename); 							#Client to Master
	binary 			GetFile(1:string filename) 								#Client to node with a copy of the file
	oneway void 	FileStorageAck(1:string filename, 2:string incomingID) 	#Node to Master after copy is made
	oneway void 	RequestFileCopy(1:string filename, 2:string nodeID)		#Replica Manager to Node containing file	
	set<string> 	GetFileList();											#Client to NodeMgr
	string 			GetLeaderIP();
	string          GetFullInfo();
	
	
}
