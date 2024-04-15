#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/shm.h>

#define PERMS 0644

typedef struct msg_buffer_t{
	long mtype;	//mtype 1 will be read by load balancer, 2 by primary server, 3 by secondary server1, 4 by secondary server2, clients will read mtype (seq+4) to receive response from server 
	int seq;
	int op;	
	char mtext[60];
	int sender; 	//0 for client,1 for load balancer,2 for server, 3 for cleanup
}msg_buffer;

int main(){
	int msqid;
	key_t key;
	
	// Generate a unique key for the message queue
	if((key = ftok("load_balancer.c",'A'))==-1){
		perror("Error in ftok() in line 25\n");
		exit(-1);
	}
	// Create message queue
	if((msqid=msgget(key,PERMS | IPC_CREAT)) == -1){
		perror("Error in msgget() in line 30\n");
		exit(-2);
	}
	
	while(1){
		msg_buffer msg;

		if(msgrcv(msqid,&msg,sizeof(msg_buffer),1,0)==-1){  
			perror("Error in msgrcv() in line 38\n");
			exit(-2);
		}
		
		if(msg.sender==0){

			if(msg.op==1 || msg.op==2){
				printf("Received a write request on file %s\n\n",msg.mtext);				
				msg.mtype = 2;
				if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
					perror("Error in msgsnd() in line 48\n");
					exit(-3);
				}
			}
			
			if(msg.op==3 || msg.op==4){
				printf("Received a read request on file %s\n\n",msg.mtext);				
				if(msg.seq % 2 != 0)	
					msg.mtype = 3;
				else	
					msg.mtype = 4;
					
				if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
					perror("Error in msgsnd() in line 61\n");
					exit(-3);
				}
			}
			
		}
		
		if(msg.sender==3){ //cleanup
		
			printf("Received termination request.\n");
			printf("Informing the servers to terminate.\n");
			
			//Sending 3 messages with diff mtypes to 3 servers i.e. primary server and two secondary servers 
			msg.mtype=2;
			if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
				perror("Error in msgsnd() in line 76\n");
				exit(-3);
			}
			msg.mtype=3;
			if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
				perror("Error in msgsnd() in line 81\n");
				exit(-3);
			}
			msg.mtype=4;
			if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
				perror("Error in msgsnd() in line 86\n");
				exit(-3);
			}
			
			sleep(5);
			
			//Delete the message queue
        		if(msgctl(msqid,IPC_RMID,NULL)==-1){  
				perror("Error in msgctl in line 94\n");
				exit(-1);
			}
			
			printf("Terminating...\n");
			exit(0);
		}
	}
}
