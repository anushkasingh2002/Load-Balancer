#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>

#define PERMS 0644

typedef struct msg_buffer_t{
	long mtype;
	int seq;
	int op;	
	char mtext[60];
	int sender; 	//0 for client,1 for load balancer,2 for server, 3 for cleanup
}msg_buffer;

int main() {
    	int msqid;
    	key_t key;

    	// Generate a unique key for the message queue
    	if ((key = ftok("load_balancer.c",'A')) == -1) {
		perror("Error in ftok() in line 23\n");
		exit(-1);
  	}

    	// Access the existing message queue with the server
    	if ((msqid = msgget(key,PERMS)) == -1) {
    		perror("Error in msgget() in line 29\n");
		exit(-2);
    	}
    
   	msg_buffer buf;
    	char choice;
    
    	do{
    		char ch[3];
    		printf("Want to terminate the application? Press Y (Yes) or N (No):\n");
    		scanf("%s", ch);
    		choice=ch[0];
    		if (choice == 'Y' || choice == 'y') {
       			buf.mtype=1;
       			buf.sender=3;
			if(msgsnd(msqid,&buf,sizeof(buf),0)==-1){
				perror("Error in msgsnd() in line 45\n");
				exit(-3);
			}
			printf("\nTermination request has been sent to the load balancer.\n");
			break;
    		} 
    		else if (choice == 'N' || choice=='n') 
        		printf("\nThe application is still running.\n");
    		else
        		printf("\nInvalid choice. \n");

    	}while(choice!='Y' || choice!='y');
    
    	return 0;
}
