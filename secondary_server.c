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
#define MAX_NODES 30
#define MAX_REQUESTS 100

char traversal[100][60];
int readers[20];

typedef struct msg_buffer_t{
	long mtype;
	int seq;
	int op;	
	char mtext[60];
	int sender; 	//0 for client,1 for load balancer,2 for server, 3 for cleanup
}msg_buffer;

typedef struct {
    int matrix[MAX_NODES][MAX_NODES];
    bool visited[MAX_NODES][MAX_NODES];
    int num_nodes;
    int start_node;
    int request;
} GraphData_DFS;

typedef struct {
    int matrix[MAX_NODES][MAX_NODES];
    int seq;
    intptr_t node;
    int num_nodes;
} GraphData_BFS;

pthread_mutex_t queueMutex[MAX_REQUESTS] = {PTHREAD_MUTEX_INITIALIZER};
pthread_mutex_t enqueueMutex[MAX_REQUESTS] = {PTHREAD_MUTEX_INITIALIZER};
pthread_mutex_t dequeueMutex[MAX_REQUESTS] = {PTHREAD_MUTEX_INITIALIZER};

int processed[MAX_REQUESTS][MAX_NODES];

int queue[MAX_REQUESTS][MAX_NODES];
int front[MAX_REQUESTS], rear[MAX_REQUESTS];

void enqueue(int node,int seq)
{
   
    if (rear[seq-1] == -1)
    {
        front[seq-1] = 0;
    }
     pthread_mutex_lock(&enqueueMutex[seq-1]);
    rear[seq-1]++;
    queue[seq-1][rear[seq-1]] = node;
     pthread_mutex_unlock(&enqueueMutex[seq-1]);
}

int dequeue(int seq)
{
    if(front[seq-1] == -1 || front[seq-1] > rear[seq-1])
    {
      return -1; 
    }
     pthread_mutex_lock(&dequeueMutex[seq-1]);
    int node = queue[seq-1][front[seq-1]];
    front[seq-1]++;
     pthread_mutex_unlock(&dequeueMutex[seq-1]);

    return node;
}

bool isQueueEmpty(int seq)
{
    bool isEmpty = front[seq-1] > rear[seq-1];
    return isEmpty;
}


int getFront(int seq)
{
    int frontNode = queue[seq-1][front[seq-1]];
    return frontNode;
}

int getQueueSize(int seq)
{
    int size = rear[seq-1] - front[seq-1] + 1;
    return size;
}

void processNode(int node,int seq)
{
	processed[seq-1][node]=1;
	char nextNode[3];
    	sprintf(nextNode,"%d ",node+1);
    	strcat(traversal[seq],nextNode);
    	//printf("%d\n", node + 1);
}

void *bfs(void *arg)
{
	GraphData_BFS *data = (GraphData_BFS *)arg;
	int node = (int)data->node;
	int seq = data->seq;
	int num = data->num_nodes;
        processNode(node,seq);

        pthread_mutex_lock(&queueMutex[seq-1]);
        for (int i = 0; i < num; i++)
        {
            if (data->matrix[node][i] == 1 && !processed[seq-1][i])
            {
                int newNode = i;
                enqueue(newNode,seq);
            }
        }
        pthread_mutex_unlock(&queueMutex[seq-1]);
        
    pthread_exit(NULL);
}

void* dfs(void* arg) {
    GraphData_DFS* graph = (GraphData_DFS*)arg;
    int start = graph->start_node;
    int deepest_vertex = start;
    pthread_t threads[MAX_NODES]; // Array to store thread IDs
    int is_last_node = 1;
    int seq = graph->request;	
	
    int num_threads = 0;
    for (int i = 0; i < graph->num_nodes; i++) {
        if (graph->matrix[start][i] && !graph->visited[start][i]) {
            deepest_vertex = i;
            GraphData_DFS* new_graph = (GraphData_DFS*) malloc(sizeof(GraphData_DFS));
            *new_graph = *graph; // Copy the current graph data
            new_graph->start_node = i;
            // Create a new thread for unvisited neighbour nodes
            new_graph->visited[start][i] = true;
            new_graph->visited[i][start] = true;
            new_graph->request = seq;
            is_last_node = 0;
            
            pthread_create(&threads[num_threads], NULL, dfs,(void *) new_graph);
            num_threads++;
        }
    }

    // Wait for all the child threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    if (is_last_node) {
    	char lastNode[3];
    	sprintf(lastNode,"%d ",start+1);
    	strcat(traversal[seq],lastNode);
        //printf("%d is a last node in this graph.\n", start);
    }

    free(graph); // Free the dynamically allocated graph data

    pthread_exit(NULL);
}

void *service(void *arg){
	int *shm,shmid;
	msg_buffer *msg = (msg_buffer *)arg;
	
	char seq_key[10];
	sprintf(seq_key, "%d" , msg->seq);
	key_t k=ftok(msg->mtext,msg->seq); 
	//key_t k=ftok(seq_key,10);	//use msg seq number to get the key 
	char lock_name[10]="/";
   	strcat(lock_name,msg->mtext);
   	sem_t *readwriteLock = sem_open(lock_name, O_CREAT, 0666, 1);
   	
	char lock_name_reader[10]="/r";
   	strcat(lock_name_reader,msg->mtext);
   	sem_t *mutex = sem_open(lock_name_reader, O_CREAT, 0666, 1);

	if((shmid= shmget(k,sizeof(int[BUF_SIZE]),0666)) ==-1){ 		//different key for each request to get a separate shared memory
		perror("Error in line 192\n");
		exit(-2);
	}
	shm = (int *)shmat(shmid,NULL,0);
	int start = shm[0]-1;
	if(shmdt(shm)==-1){
		perror("Error in line 198\n");
		exit(-3);
	}
	int num;
        sscanf(msg->mtext, "G%d", &num);
        
        
	if(msg->op == 3){
		printf("Received DFS request on the graph file %s\n\n", msg->mtext);
		GraphData_DFS* graph_data = (GraphData_DFS*)malloc(sizeof(GraphData_DFS));
	
		graph_data->start_node = start;
		graph_data->request = msg->seq;
		    	
    		sem_wait(mutex);
        	readers[num-1]++;
        	if (readers[num-1] == 1) {
        	    // First reader blocks writers
        	    sem_wait(readwriteLock);
        	}
        	sem_post(mutex);
        		
		FILE *graph= fopen(msg->mtext, "r");
    		fscanf(graph, "%d", &graph_data->num_nodes);
 	  
    		for (int i = 0; i < graph_data->num_nodes; i++) {
        		for (int j = 0; j < graph_data->num_nodes; j++) {
        		    fscanf(graph, "%d", &graph_data->matrix[i][j]);
        		    //printf("%d ",graph_data->matrix[i][j]);
        		}
    		}	
    		
    		fclose(graph);
 	
 		sem_wait(mutex);
        	readers[num-1]--;
        	if (readers[num-1] == 0) {
        	    // Last reader unblocks writers
        	    sem_post(readwriteLock);
        	}
        	sem_post(mutex);
    				
		pthread_t thread; 
	
		pthread_create(&thread,NULL,dfs,(void *)graph_data);	
		pthread_join(thread,NULL);
	}
	
	if(msg->op == 4){
		printf("Received BFS request on the graph file %s\n\n", msg->mtext);
		GraphData_BFS* graph_data = (GraphData_BFS*)malloc(sizeof(GraphData_BFS));
	
		graph_data->node = (intptr_t)start;
    	
    		sem_wait(mutex);
        	readers[num-1]++;
        	if (readers[num-1] == 1) {
        	    // First reader blocks writers
        	    sem_wait(readwriteLock);
        	}
        	sem_post(mutex);
        		
		FILE *graph= fopen(msg->mtext, "r");
    		fscanf(graph, "%d", &graph_data->num_nodes);
 	  
    		for (int i = 0; i < graph_data->num_nodes; i++) {
        		for (int j = 0; j < graph_data->num_nodes; j++) {
        		    fscanf(graph, "%d", &graph_data->matrix[i][j]);
        		    //printf("%d ",graph_data->matrix[i][j]);
        		}
    		}	
    		
    		fclose(graph);
 	
 		sem_wait(mutex);
        	readers[num-1]--;
        	if (readers[num-1] == 0) {
        	    // Last reader unblocks writers
        	    sem_post(readwriteLock);
        	}
        	sem_post(mutex);
        	
		int seq = msg->seq;
		graph_data->seq = seq;
		
		front[seq-1]=-1;
		rear[seq-1]=-1;
		
		enqueue(start,seq);
 		
  		while (!isQueueEmpty(seq))
    		{
       			int n = getFront(seq);

        		if (n == -1)
        		{
            			break; // No more nodes to process
        		}
	
			int size = getQueueSize(seq);
			//printf("Size:%d\n",size);
        		// Create a new thread for each node
        		pthread_t threads[size];
        		GraphData_BFS gdata[size];
        		for(int i=0;i<size;i++){
        			gdata[i] = *graph_data;
        			gdata[i].node = dequeue(seq);
        			pthread_create(&threads[i], NULL, bfs,(void *)&gdata[i]);
        		}
        		for(int i=0;i<size;i++)
       				pthread_join(threads[i],NULL);
       			
		}		
	}

	int msqid;
	key_t key;
	// Generate a unique key for the message queue
	if((key = ftok("load_balancer.c",'A'))==-1){
		perror("Error in ftok() in line 327\n");
		exit(-1);
	}
	// Access the existing message queue with the load balancer
	if((msqid=msgget(key,PERMS)) == -1){
		perror("Error in msgget() in line 332\n");
		exit(-2);
	}
	msg_buffer response;
	response.mtype = msg->seq+4;
	response.sender = 2;
	traversal[msg->seq][59]='\0';
	strcpy(response.mtext,traversal[msg->seq]); 
	if(msgsnd(msqid,&response,sizeof(response),0)==-1){
		perror("Error in msgsnd() in line 341\n");
		exit(-3);
	}
	//printf("%s\n",traversal);
	pthread_exit(NULL);
}

int main(){
	int msqid;
	key_t key;
	
	int server_no;
	printf("Specify whether this is secondary server 1 or 2:\n");
	scanf("%d",&server_no);
	// Generate a unique key for the message queue
	if((key = ftok("load_balancer.c",'A'))==-1){
		perror("Error in ftok() in line 357\n");
		exit(-1);
	}
	// Access the existing message queue with the load balancer
	if((msqid=msgget(key,PERMS)) == -1){
		perror("Error in msgget() in line 362\n");
		exit(-2);
	}
	pthread_t service_threads[MAX_THREADS]; // Array to store thread IDs
	pthread_attr_t threads_attr[MAX_THREADS]; 
	int count = 0; // Counter to keep track of created threads
	msg_buffer msg[101];
	while(true){
		if(server_no==1){
			if(msgrcv(msqid,&msg[count],sizeof(msg_buffer),3,0)==-1){
				perror("Error in msgrcv() in line 372\n");
				exit(-3);		
			}
		}
		if(server_no==2){
			if(msgrcv(msqid,&msg[count],sizeof(msg_buffer),4,0)==-1){
				perror("Error in msgrcv() in line 378\n");
				exit(-3);		
			}
		}
		//printf("%s\n",msg.mtext);
		if(msg[count].sender==0){					//If client has originally sent a request, then create new thread
			pthread_create(&service_threads[count],NULL,service,(void *)&msg[count]);	//pass the msg that is received from message queue			
			count++;			
		}
		
		if(msg[count].sender==3){
			//If cleanup has originally sent the request, then wait for all threads 
			for(int i=0;i<count;i++)
				pthread_join(service_threads[i],NULL);
			printf("Terminating...\n");
			exit(0);
		}
	}
	return 0;
}
