#include <cassert>
#include <cerrno>
#include <cstdio>

#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <execinfo.h>

#include <arpa/inet.h>
#include <netinet/in.h> 
#include <net/if.h>
#include <sys/ioctl.h>

#include <unistd.h>
#include <sys/syscall.h>
#include <signal.h>
#include <mpi.h>
#include "ucx_client.h"


int mpi_rank, nClient=0;	// rank and size of MPI

extern FSSERVERLIST UCXFileServerListLocal;
void Read_UCX_FS_Param(void) {
	char szFileName[128], *szServerConf=NULL;
	FILE *fIn;
	int i, j, nItems;
	printf("DBG> Read_UCX_FS_Param\n");
	fIn = fopen(UCX_FS_PARAM_FILE, "r");
	if(fIn == NULL)	{
		printf("ERROR> Failed to open file %s\nQuit\n", UCX_FS_PARAM_FILE);
	}
	nItems = fscanf(fIn, "%d%d", &(UCXFileServerListLocal.nFSServer), &(UCXFileServerListLocal.nNUMAPerNode));
	if(nItems != 2)	{
		printf("ERROR> Failed to read nFSServer and nNUMAPerNode for UCX.\n");
		fclose(fIn);
		exit(1);
	}

	for(i=0; i<UCXFileServerListLocal.nFSServer; i++)	{
		nItems = fscanf(fIn, "%s%d", UCXFileServerListLocal.FS_List[i].szIP, &(UCXFileServerListLocal.FS_List[i].port));
		if(nItems != 2)	{
			printf("ERROR> Failed to read ip port information of ucx file server.\nQuit\n");
			fclose(fIn);
			exit(1);
		}
		if (pthread_mutex_init(&(UCXFileServerListLocal.FS_List[i].fs_qp_lock), NULL) != 0) { 
			printf("\n pUCXFileServerList mutex init failed\n"); 
			exit(1);
		}
		UCXFileServerListLocal.FS_List[i].nQP = 0;
	}
	fclose(fIn);
}

void Init_UCX_Client()  {
    printf("DBG> Begin Init_UCX_Client()\n");
    CLIENT_UCX::Init_UCX_Env();
    Read_UCX_FS_Param();
}

#define IO_CNT 500
#define MULTI_THREAD_NUM 1
void Wait_For_Ack(CLIENT_UCX* pClientUCX, void* addr) {
    // printf("Wait_For_Ack: %p\n", addr);
    while(*((int*)addr) != 1) {
        ucp_worker_progress(pClientUCX->ucp_worker);
    }
}

inline int generate_test_string(char *str, int size)
{
    int i;
    int start = *((int*)str);
    printf("mpi_rank: %d, generate_test_string %d\n", mpi_rank, start);
    for (i = 4; i < (size - 1); ++i) {
        // printf("haha%d\n", i);
        *(str + i) = (char)('A' + ((start + i) % 26));
    }
    // str[i] = '\0';
    return 0;
}

static void* Func_thread_send(void *pParam) {
    CLIENT_UCX* pClientUCX = (CLIENT_UCX*)pParam;
    char buffer[BLOCK_SIZE];
    unsigned char b[1];
    b[0] = TAG_NEW_REQUEST;
    for(int i = 0; i < IO_CNT; i++) {
        // sprintf(buffer, "%d", i);
        int* intptr = (int*)buffer;
        *intptr = i;
        generate_test_string(buffer, BLOCK_SIZE);
        memset(pClientUCX->ucx_rem_buff, 0, sizeof(int));
        pClientUCX->UCX_Put(buffer, (void*)(pClientUCX->remote_addr_IO_CMD), pClientUCX->pal_remote_mem.rkey, BLOCK_SIZE);
        pClientUCX->UCX_Put(b, (void*)(pClientUCX->remote_addr_new_msg), pClientUCX->pal_remote_mem.rkey, 1);
        Wait_For_Ack(pClientUCX, pClientUCX->ucx_rem_buff);
    }
    return NULL;
}

int main() {
    MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &nClient);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    Init_UCX_Client();
    int nFSServer = UCXFileServerListLocal.nFSServer;
    CLIENT_UCX clientUCX[nFSServer];
    for(int i = 0; i < nFSServer; i++) {
        clientUCX[i].Setup_UCP_Connection(i);
    }
    pthread_t send_threads[nFSServer * MULTI_THREAD_NUM];
    for(int i = 0; i < nFSServer * MULTI_THREAD_NUM; i++) {
        if(pthread_create(&(send_threads[i]), NULL, Func_thread_send, &clientUCX[i % nFSServer])) {
            fprintf(stderr, "Error creating thread\n");
		    return 1;
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);
    for(int i = 0; i < nFSServer * MULTI_THREAD_NUM; i++) {
        if(pthread_join(send_threads[i], NULL)) {
            fprintf(stderr, "Error joining thread.\n");
            return 2;
        }
    }
    MPI_Finalize();

}