namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
   int JoinGroup();
   string GetLeaderId();
   binary GetMembershipList();
   int ReceiveElectionMessage();
   void ReceiveCoordinationMessage(1:string leaderId);
}

