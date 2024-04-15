#include <stdio.h>
#include <stdlib.h>
#include <sys/ipc.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <stdbool.h>
#include <string.h>

#define PERMS 0644
#define BUF_SIZE 901
 
typedef struct msg_buffer_t{
	long mtype;
	int seq;
	int op;	
	char mtext[60];
	int sender; 	//0 for client,1 for load balancer,2 for server, 3 for cleanup
}msg_buffer;

int main(){
	printf("1. Add a new graph to the database\n");
	printf("2. Modify an existing graph of the database\n");
	printf("3. Perform DFS on an existing graph of the database\n");
	printf("4. Perform BFS on an existing graph of the database\n");
	
	int msqid;
	key_t key;
	
	// Generate a unique key for the message queue
	if((key = ftok("load_balancer.c",'A'))==-1){
		perror("Error in ftok() in line 33 \n");
		exit(-1);
	}
	// Access the existing message queue with the load balancer
	if((msqid=msgget(key,PERMS)) == -1){
		perror("Error in msgget() in line 38\n");
		exit(-2);
	}
	
	while(true){
		int seq_no,op_no;
		char file_name[30];
		msg_buffer msg;
		int *shm,shmid;
		
		msg.mtype=1;				//load balancer reads msg of mtype 1 only
		msg.sender=0;
		printf("Enter Sequence Number:\n");
		scanf("%d",&msg.seq);
		printf("Enter Operation Number:\n");
		scanf("%d",&msg.op);
		printf("Enter Graph File Name:\n");
		scanf("%s",msg.mtext);
		int len=strlen(msg.mtext);
		if(msg.mtext[len-1] == '\n')
			msg.mtext[len-1]='\0';
		
		char seq_key[10];
		sprintf(seq_key, "%d" , msg.seq);
		key_t k=ftok(msg.mtext,msg.seq); //use msg seq number to generate the key
		if((shmid= shmget(k,sizeof(int[BUF_SIZE]),0666 | IPC_CREAT)) ==-1){ 		//different key for each request to get a separate shared memory
			perror("Error in shmget in line 64\n");
			exit(-2);
		}
		shm = (int *)shmat(shmid,NULL,0);
		
		int nodes,start_node;
		msg_buffer response;
		switch(msg.op){
			case 1:	printf("Enter number of nodes of the graph\n");
				scanf("%d",&nodes);
				shm[0]=nodes;
				printf("Enter adjacency matrix, each row on a separate line and elements of a single row separated by whitespace characters\n");
				for(int i=1;i<=nodes*nodes;i++)
					scanf("%d",&shm[i]);
				
				if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
					perror("Error in msgsnd() in line 80\n");
					exit(-3);
				}
						
				if(msgrcv(msqid,&response,sizeof(response),msg.seq+4,0)==-1){
					perror("Error in msgrcv() in line 85\n");
					exit(-3);		
				}
				printf("\n");
				printf("%s\n",response.mtext);
				break;
				
			case 2: printf("Enter number of nodes of the graph\n");
				scanf("%d",&nodes);
				shm[0]=nodes;
				
				printf("Enter adjacency matrix, each row on a separate line and elements of a single row separated by whitespace characters\n");
				for(int i=1;i<=nodes*nodes;i++){
					scanf("%d",&shm[i]);
				}
				if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
					perror("Error in msgsnd() in line 101\n");
					exit(-3);
				}
				if(msgrcv(msqid,&response,sizeof(response),msg.seq+4,0)==-1){
					perror("Error in msgrcv() in line 105\n");
					exit(-3);		
				}
				printf("\n");
				printf("%s\n",response.mtext);
				break;
			
			case 3:	printf("Enter starting vertex\n");
				scanf("%d",&start_node);
				shm[0]=start_node;
				if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
					perror("Error in msgsnd() in line 116\n");
					exit(-3);
				}
				if(msgrcv(msqid,&response,sizeof(response),msg.seq+4,0)==-1){
					perror("Error in msgrcv() in line 120\n");
					exit(-3);		
				}
				printf("\n");
				printf("%s\n",response.mtext);
				break;
				
			case 4:	printf("Enter starting vertex\n");
				scanf("%d",&start_node);
				shm[0]=start_node;
				if(msgsnd(msqid,&msg,sizeof(msg),0)==-1){
					perror("Error in msgsnd() in line 131\n");
					exit(-3);
				}
				if(msgrcv(msqid,&response,sizeof(response),msg.seq+4,0)==-1){
					perror("Error in msgrcv() in line 135\n");
					exit(-3);		
				}
				printf("\n");
				printf("%s\n",response.mtext);
				break;
			
		       default: 
		       		break;
		}
		//Detach the shared memory segment
		if(shmdt(shm)==-1){
			perror("Error in line 146\n");
			exit(-3);
		}
		//Delete the shared memory segment 
		if(shmctl(shmid,IPC_RMID,0)==-1){
			perror("Error in line 150\n");
			exit(-4);
		}
		printf("\n");		
	}

}
