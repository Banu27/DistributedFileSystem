namespace java edu.uiuc.cs425

typedef i32 int

service CommandInterface{
   int JoinGroup();
   binary GetMembershipList();
   void ReceiveElectionMessage();
   void ReceiveCoordinationMessage();
}

