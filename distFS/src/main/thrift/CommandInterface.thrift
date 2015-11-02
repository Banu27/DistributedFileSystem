namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
   int JoinGroup();
   string GetLeaderId();
   binary GetMembershipList();
   void ReceiveElectionMessage();
   void AddBlock(1:int size, 2:string blockID, 3:binary payload, 4:bool replicate);
   
   void ReceiveCoordinationMessage(1:string leaderId);
   int IsLeaderAlive();
}
