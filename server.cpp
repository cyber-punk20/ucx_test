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
#include "common_info.h"
#include "ucx_rma.h"

#define PORT 8888
#define UCX_PORT 12599

int mpi_rank, nFSServer=0;	// rank and size of MPI
int nNUMAPerNode=1;	// number of numa nodes per compute node
int Ucx_Server_Started = 0;
SERVER_RDMA Server_ucx;

static void sigsegv_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[256];

	sprintf(szMsg, "\n\n\n\n\n\n\n\n\nGot signal %d (SIGSEGV) rank = %d tid = %d\n\n\n\n\n\n\n", siginfo->si_signo, mpi_rank, syscall(SYS_gettid));
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	fsync(STDERR_FILENO);
	sleep(3000);
//	if(org_segv)	org_segv(sig, siginfo, uc);
//	else	exit(1);
}

typedef void (*org_sighandler)(int sig, siginfo_t *siginfo, void *ptr);
static org_sighandler org_int=NULL;

static void sigint_handler(int sig, siginfo_t *siginfo, void *uc)
{
	char szMsg[]="Received sigint.\n";
	write(STDERR_FILENO, szMsg, strlen(szMsg));
	fsync(STDERR_FILENO);

        if(org_int)     org_int(sig, siginfo, uc);
        else    exit(0);
}
typedef	struct	{
	struct in_addr sin_addr;
	int port;
	int ucx_port;
	char szIP[16];
}FS_SEVER_INFO;
FS_SEVER_INFO ThisNode;
FS_SEVER_INFO AllFSNodes[MAX_FS_UCX_SERVER];

void Get_Local_Server_Info(void)
{
	int fd;
	struct ifreq ifr;
	
	fd = socket(AF_INET, SOCK_DGRAM, 0);
	ifr.ifr_addr.sa_family = AF_INET;
	strncpy(ifr.ifr_name, IB_DEVICE, strlen(IB_DEVICE)+1);
	ioctl(fd, SIOCGIFADDR, &ifr);
	close(fd);
	ThisNode.sin_addr = ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr;
	sprintf(ThisNode.szIP, "%s", inet_ntoa(ThisNode.sin_addr));
	ThisNode.port = PORT + (mpi_rank % nNUMAPerNode);
	ThisNode.ucx_port = UCX_PORT + (mpi_rank % nNUMAPerNode);
}


static void* Func_thread_ucx_server(void *pParam) {
	SERVER_RDMA *pServer_ucx;
	int i;

	pServer_ucx = (SERVER_RDMA *)pParam;
	pServer_ucx->Init_Server_UCX_Env(DEFAULT_REM_BUFF_SIZE);
	pServer_ucx->Init_Server_Memory(2048, ThisNode.ucx_port);

	Ucx_Server_Started = 1;	// active the flag: Server started running!!!
	printf("Rank = %d. UCX Server is started.\n", mpi_rank);
	pServer_ucx->Socket_Server_Loop();
	
	return 0;
}

static void* Func_thread_UCX_Polling_New_Msg(void *pParam)
{
	SERVER_RDMA *pServer_ucx;

	pServer_ucx = (SERVER_RDMA *)pParam;
	while(1)	{
		sleep(1);
		if(pServer_ucx->pUCX_Data)	{
			break;
		}
	}
	sleep(1);
	while(1)	{
		pServer_ucx->ScanNewMsg();
	}
	return NULL;
}

int main(int argc, char **argv) {
    struct sigaction act, old_action;
	
    // Set up sigsegv handler
    memset (&act, 0, sizeof(act));
    act.sa_flags = SA_SIGINFO;
	
    act.sa_sigaction = sigsegv_handler;
    if (sigaction(SIGSEGV, &act, &old_action) == -1) {
        perror("Error: sigaction");
        exit(1);
    }

    act.sa_sigaction = sigint_handler;
    if (sigaction(SIGINT, &act, &old_action) == -1) {
        perror("Error: sigaction");
       exit(1);
    }
    if( (old_action.sa_handler != SIG_DFL) && (old_action.sa_handler != SIG_IGN) )  {
            org_int = old_action.sa_sigaction;
    }

    MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &nFSServer);
	MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    Get_Local_Server_Info();
	MPI_Allgather(&ThisNode, sizeof(FS_SEVER_INFO), MPI_CHAR, AllFSNodes, sizeof(FS_SEVER_INFO), MPI_CHAR, MPI_COMM_WORLD);
    FILE *fOut;
	if(mpi_rank == 0)	{
		printf("INFO> There are %d servers.\n", nFSServer);
		fOut = fopen(UCX_FS_PARAM_FILE, "w");
		if(fOut == NULL)	{
			printf("ERROR> Fail to open file: %s\nQuit.\n", UCX_FS_PARAM_FILE);
			exit(1);
		}
		fprintf(fOut, "%d %d\n", nFSServer, nNUMAPerNode);
		for(int i=0; i<nFSServer; i++)	{
			printf("     %d %s %d\n", i, AllFSNodes[i].szIP, AllFSNodes[i].ucx_port);
			fprintf(fOut, "%s %d\n", AllFSNodes[i].szIP, AllFSNodes[i].ucx_port);
		}
		fclose(fOut);
	}
    pthread_t thread_ucx_polling_newmsg, thread_ucx_server;
    if(pthread_create(&(thread_ucx_server), NULL, Func_thread_ucx_server, &Server_ucx)) {
		fprintf(stderr, "Error creating thread\n");
		return 1;
	}
    while(1)	{
		if(Ucx_Server_Started)	break;
	}
	MPI_Barrier(MPI_COMM_WORLD);

    
    if(pthread_create(&(thread_ucx_polling_newmsg), NULL, Func_thread_UCX_Polling_New_Msg, &Server_ucx)) {
		fprintf(stderr, "Error creating thread thread_ucx_polling_newmsg\n");
		return 1;
	}
	printf("DBG> Rank = %d,  started Func_thread_UCX_Polling_New_Msg().\n", mpi_rank);

	// signal(SIGALRM, sigalarm_handler); // Register signal handler
    if(pthread_join(thread_ucx_polling_newmsg, NULL)) {
		fprintf(stderr, "Error joining thread.\n");
		return 2;
	}
    if(pthread_join(thread_ucx_server, NULL)) {
		fprintf(stderr, "Error joining thread thread_ucx_server.\n");
		return 2;
	}


    MPI_Finalize();
	
	return 0;

}