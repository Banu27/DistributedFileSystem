namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
   int JoinGroup();
   string GetLeaderId();
   binary GetMembershipList();
   oneway void ReceiveElectionMessage();
   int AddFile(1:int size, 2:string fileID, 3:binary payload, 4:bool replicate);
   oneway void DeleteFile(1:string fileID);
   oneway void ReceiveCoordinationMessage(1:string leaderId);
   int IsLeaderAlive();
}
