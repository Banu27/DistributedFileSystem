namespace java edu.uiuc.cs425

typedef i32 int

service MemberIntroducer{
   int JoinGroup(); //Introducer
   string GetLeaderId(); //Introducer
   binary GetMembershipList(); //Introducer
   int ReceiveElectionMessage(); //Election
   void ReceiveCoordinationMessage(1:string leaderId); //Election
}
