namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
   int JoinGroup();
   string GetLeaderId();
   binary GetMembershipList();
   void ReceiveElectionMessage();
   int AddFile(1:int size, 2:string fileID, 3:binary payload, 4:bool replicate);
   void DeleteFile(1:string fileID);
   void ReceiveCoordinationMessage(1:string leaderId);
   int IsLeaderAlive();
}
