#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <sys/types.h>
#include <sys/shm.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>

#define PERMS 0644
#define BUF_SIZE 901
#define MAX_THREADS 100
 
typedef struct msg_buffer_t{
	long mtype;
	int seq;
	int op;	
	char mtext[60];
	int sender; 	//0 for client,1 for load balancer,2 for server, 3 for cleanup
}msg_buffer;

void *modifyGraph(void *arg){
	int *shm,shmid;
	msg_buffer *msg = (msg_buffer *)arg;
	char seq_key[10];
	sprintf(seq_key, "%d" , msg->seq);
	key_t k=ftok(msg->mtext,msg->seq); 
	//key_t k=ftok(seq_key,10);	//use msg seq number to get the key 
	char lock_name[10]="/";
   	strcat(lock_name,msg->mtext);
   	sem_t *readwriteLock = sem_open(lock_name, O_CREAT, 0666, 1);
   	
	if((shmid= shmget(k,sizeof(int[BUF_SIZE]),0666)) ==-1){ 		//different key for each request to get a separate shared memory
		perror("Error in line 38\n");
		exit(-2);
	}
	shm = (int *)shmat(shmid,NULL,0);
	int nodes=shm[0];
	
	int adj[nodes*nodes];
	for(int i=0;i<nodes*nodes;i++)
		adj[i]=shm[i+1];
		
	if(shmdt(shm)==-1){
		perror("Error in line 49\n");
		exit(-3);
	}
	
	sem_wait(readwriteLock);	
	FILE *graph = fopen(msg->mtext,"w");
	fputc(nodes+'0',graph);
	fputc('\n',graph);
	int index=0;
	for(int i=0;i<nodes;i++){
		for(int j=0;j<nodes;j++){
			fputc(adj[index++]+'0',graph);
			fputc(' ',graph);
		}
		fputc('\n',graph);
	}	
	fclose(graph);
	sem_post(readwriteLock);
	
	int msqid;
	key_t key;
	// Generate a unique key for the message queue
	if((key = ftok("load_balancer.c",'A'))==-1){
		perror("Error in ftok() in line 72\n");
		exit(-1);
	}
	// Access the existing message queue with the load balancer
	if((msqid=msgget(key,PERMS)) == -1){
		perror("Error in msgget() in line 77\n");
		exit(-2);
	}
	msg_buffer response;
	response.mtype = msg->seq+4;
	response.sender = 2;
	if(msg->op == 1)
		strcpy(response.mtext,"File successfully added.");
	if(msg->op == 2)
		strcpy(response.mtext,"File successfully modified.");
		
	if(msgsnd(msqid,&response,sizeof(response),0)==-1){
		perror("Error in msgsnd() in line 89\n");
		exit(-3);
	}
	pthread_exit(NULL);
}

int main(){
	int msqid;
	key_t key;
	
	// Generate a unique key for the message queue
	if((key = ftok("load_balancer.c",'A'))==-1){
		perror("Error in ftok() in line 101\n");
		exit(-1);
	}
	// Access the existing message queue with the load balancer
	if((msqid=msgget(key,PERMS)) == -1){
		perror("Error in msgget() in line 106\n");
		exit(-2);
	}
	pthread_t threads[MAX_THREADS]; // Array to store thread IDs

	int count = 0; // Counter to keep track of created threads
	msg_buffer msg[101];
	while(true){
		
		if(msgrcv(msqid,&msg[count],sizeof(msg_buffer),2,0)==-1){
			perror("Error in msgrcv() in line 116\n");
			exit(-3);		
		}
		
		if(msg[count].sender==0){					//If client has originally sent a request, then create new thread
			printf("Received a write request on the file %s\n",msg[count].mtext);
			pthread_create(&threads[count],NULL,modifyGraph,(void *)&msg[count]);	//pass the msg that is received from message queue
			count++;
		}
		
		if(msg[count].sender==3){					//If cleanup has originally sent the request, then wait for all threads 
			for(int i=0;i<count;i++)
				pthread_join(threads[i],NULL);
			printf("Terminating...\n");
			exit(0);
		}
	}
	return 0;
}
