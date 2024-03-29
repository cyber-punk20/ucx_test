#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <string.h>
#include <errno.h>
#include <vector>
#include "ucx_rma.h"

int nUCXNewMsg, UCXNewMsgList[MAX_UCX_NEW_MSG];
extern int mpi_rank, nFSServer;	// rank and size of MPI

long int nSizeUCXReg;

void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status)
{
    printf("mpi_rank %d: error handling callback was invoked with status %d (%s)\n",
           mpi_rank, status, ucs_status_string(status));
}

typedef	struct	{
	int fd;
	int idx;	// idx of QP
	SERVER_RDMA *pServer_UCX;
}UCXPARAM, *PUCXPARAM;

typedef struct {
    int* idxList;
    int size;
    unsigned char *p_shm_IO_Cmd_Msg;
    SERVER_RDMA* pServer;
}CHECKPARAM, * PCHECKPARAM;

SERVER_RDMA::SERVER_RDMA(void)
{
    if(pthread_mutex_init(&process_lock, NULL) != 0) { 
        printf("\n mutex process_lock init failed\n"); 
        exit(1);
    }
    nAllUCXNewMsg = 0;
	nPreAllUCXNewMsg = 0;
}

SERVER_RDMA::~SERVER_RDMA(void)
{
	pthread_mutex_destroy(&process_lock);
    free(rkey_buffer);
    ucp_mem_unmap(ucp_main_context, mr_shm_global);
    ucp_worker_destroy(ucp_main_worker);
    ucp_cleanup(ucp_main_context);
}

void SERVER_RDMA::Init_Server_UCX_Env(int remote_buff_size) {
    ucp_main_context = NULL;
    ucp_main_worker = NULL;
    Init_Context(&ucp_main_context, &ucp_main_worker);
    printf("Rank = %d. Init_Server_UCX_Env.\n", mpi_rank);
    if(!ucp_main_context) {
        fprintf(stderr, "SERVER_RDMA Failure: No HCA can use.\n");
		exit(1);
    }
    
	for(int i=0; i<NUM_THREAD_IO_WORKER; i++)	{
        Init_Worker(ucp_main_context, &ucp_data_worker[i]);
		assert(ucp_data_worker[i] != NULL);
	}
    
}

void SERVER_RDMA::Clean_UCX_Env(void) {
    for(int i=0; i<NUM_THREAD_IO_WORKER; i++) {
        if(ucp_data_worker[i]) {
            ucp_worker_destroy(ucp_data_worker[i]);
            ucp_data_worker[i] = NULL;
        }
    }
}

ucs_status_t SERVER_RDMA::server_create_ep(ucp_worker_h data_worker,
                                     ucp_address_t* peer_address,
                                     ucp_ep_h *peer_ep) {
    ucp_ep_params_t ep_params;
    ucs_status_t    status;

    /* Server creates an ep to the client on the data worker.
     * This is not the worker the listener was created on.
     * The client side should have initiated the connection, leading
     * to this ep's creation */
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address    = peer_address;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = NULL;

    status = ucp_ep_create(data_worker, &ep_params, peer_ep);
    if (status != UCS_OK) {
        fprintf(stderr, "mpi_rank %d failed to create an endpoint on the server: (%s)\n", mpi_rank,
                ucs_status_string(status));
    } else {
        fprintf(stdout, "mpi_rank %d succeed to create an endpoint on the server: (%s)\n", mpi_rank,
                ucs_status_string(status));
    }

    return status;
}

void SERVER_RDMA::Socket_Server_Loop() {
    int i, epoll_ret;
	struct epoll_event ev;
	struct signalfd_siginfo info;

	// block all signals. we take signals synchronously via signalfd
	sigset_t all;
	// signals that we'll accept synchronously via signalfd */
//	int sigs[] = {SIGIO,SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGALRM};
	int sigs[] = {SIGIO,SIGHUP,SIGTERM,SIGINT,SIGQUIT,SIGUSR1};
	
	sigfillset(&all);
	sigprocmask(SIG_SETMASK,&all,NULL);
	
	// a few signals we'll accept via our signalfd
	sigset_t sw;
	sigemptyset(&sw);
	for(i=0; i < sizeof(sigs)/sizeof(*sigs); i++) sigaddset(&sw, sigs[i]);
	
	if (Setup_Listener()) goto done;
	
	/* create the signalfd for receiving signals */
	sock_signal_fd = signalfd(-1, &sw, 0);
	if (sock_signal_fd == -1) {
		fprintf(stderr,"signalfd: %s\n", strerror(errno));
		goto done;
	}
	
	// set up the epoll instance
	sock_epoll_fd = epoll_create(1); 
	if (sock_epoll_fd == -1) {
		fprintf(stderr,"epoll: %s\n", strerror(errno));
		goto done;
	}
	
	// add descriptors of interest
	if (Add_Epoll(EPOLLIN, sock_fd))        goto done; // listening socket
	if (Add_Epoll(EPOLLIN, sock_signal_fd)) goto done; // signal socket
	
	while ( 1 ) {
		epoll_ret = epoll_wait(sock_epoll_fd, &ev, 1, -1);
		if(epoll_ret <= 0)	{
			if(errno == EINTR)	continue;
			else break;
		}

		// if a signal was sent to us, read its signalfd_siginfo
		if (ev.data.fd == sock_signal_fd) { 
			if (read(sock_signal_fd, &info, sizeof(info)) != sizeof(info)) {
				fprintf(stderr,"ERROR> ucx failed to read signal fd buffer\n");
				continue;
			}
			else if(info.ssi_signo == SIGTERM)	{
				fprintf(stderr,"Got signal %d (SIGTERM)\n", info.ssi_signo);
			}
			else	{
				fprintf(stderr,"Got signal %d\n", info.ssi_signo);  
			}
			goto done;
		}
		
		/* regular POLLIN. handle the particular descriptor that's ready */
		assert(ev.events & EPOLLIN);
//		fprintf(stderr,"INFO> handle POLLIN on fd %d\n", ev.data.fd);
		if (ev.data.fd == sock_fd) Accept_Client();
		else Drain_Client(ev.data.fd);
	}
	
	fprintf(stderr, "epoll_wait: %s\n", strerror(errno));
	
done:   /* we get here if we got a signal like Ctrl-C */	
	if (sock_epoll_fd != -1) close(sock_epoll_fd);
	if (sock_signal_fd != -1) close(sock_signal_fd);
}


int SERVER_RDMA::FindFirstAvailableQP(void)
{
	int i, idx = -1, Done=0;

	if(FirstAV_QP < 0)	{
		return FirstAV_QP;
	}
	idx = FirstAV_QP;
	FirstAV_QP = -1;

	for(i = /*MAX(idx+1,nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*/idx + 1; i<max_qp; i++)	{
		if(pUCX_Data[i].ucp_data_worker == NULL)	{
			FirstAV_QP = i;
			break;
		}
	}
	if(FirstAV_QP < 0)	{
		printf("WARNING> All ucp_data_workers are used.\n");
	}

	return idx;
}


void* Func_thread_Finish_UCX_Setup(void *pParam)
{
	int fd, idx, nBytes, idx_Queue, idx_JobRec;
	UCXPARAM *pUCXParam;
	SERVER_RDMA *pServer_UCX;
	unsigned long long jobid_hash, jobid_cip_ctid_hash;	
	UCX_GLOBAL_ADDR_DATA Global_Addr_Data;
	UCX_DATA_SEND_BY_SERVER *pData_to_send=NULL;
	UCX_DATA_SEND_BY_CLIENT *pData_to_recv=NULL;

	pUCXParam = (UCXPARAM *)pParam;

	fd = pUCXParam->fd;
	idx = pUCXParam->idx;
	pServer_UCX = pUCXParam->pServer_UCX;

	pData_to_recv = (UCX_DATA_SEND_BY_CLIENT *)((char*)pParam + sizeof(UCXPARAM));
	pData_to_send = (UCX_DATA_SEND_BY_SERVER *)((char*)pParam + sizeof(UCXPARAM) + sizeof(UCX_DATA_SEND_BY_CLIENT) );

	pServer_UCX->pUCX_Data[idx].bTimeout = 0;
	pServer_UCX->pUCX_Data[idx].bServerReady = 1;
	
	free(pParam);

	return NULL;
}

int SERVER_RDMA::Accept_Client()
{
	int fd, nBytes, idx, idx_fd, one=1;
	unsigned int *p_token;
	struct sockaddr_in in;
	socklen_t sz = sizeof(in);
	char szBuff[128];
	
	fd = accept(sock_fd,(struct sockaddr*)&in, &sz);
	if (fd == -1) {
		printf("INFO> UCX accept: %s\n", strerror(errno)); 
	}
	else	{
		if(setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)	perror("setsockopt(2) error");
		if (sizeof(in) == sz) {
//			printf("INFO> connection fd %d from %s:%d\n", fd, inet_ntoa(in.sin_addr), (int)ntohs(in.sin_port));
		}
		
		
		nConnectionAccu++;
		pthread_mutex_lock(&process_lock);
		idx = FindFirstAvailableQP();
		assert(idx >= 0);
		nQP++;

		if(IdxLastQP < idx)	{
			IdxLastQP = idx;
			IdxLastQP64 = Align64_Int(IdxLastQP+1);	// +1 is needed since IdxLastQP is included!
		}

		AllocateUCPDataWorker(idx);

		if( FirstAV_QP > (nQP+NUM_THREAD_IO_WORKER_INTER_SERVER) )	{
			printf("RANK %d DBG> UCX Something wrong! FirstAV_QP: %d nQP: %d %d\n", mpi_rank, FirstAV_QP, nQP, nQP+NUM_THREAD_IO_WORKER_INTER_SERVER);
		}

		pthread_mutex_unlock(&process_lock);
		idx_fd = p_Hash_socket_fd->DictInsert(fd, idx, &elt_list_socket_fd, &ht_table_socket_fd);

		if(Add_Epoll(EPOLLIN, fd) == -1) {
			close(fd);
			fd = -1;
		}
	}

	return fd;
}

void Func_thread_Check_Data(void *pParam) {
    CHECKPARAM* pCheckParam;
    pCheckParam = (CHECKPARAM*)pParam;
    SERVER_RDMA* pServer = pCheckParam->pServer;
    for(int i = 0; i < pCheckParam->size; i++) {
        int idx_ucx = pCheckParam->idxList[i];
        bool res = check_test_string((char*)pCheckParam->p_shm_IO_Cmd_Msg + BLOCK_SIZE * idx_ucx, BLOCK_SIZE);
        if(res) {
            fprintf(stdout, "idx_ucx: %d pass test\n", idx_ucx);
        } else {
            fprintf(stdout, "idx_ucx: %d fail test\n", idx_ucx);
        }
        int ack = 1;
        pServer->UCX_Put(idx_ucx, &ack, (void*)pServer->pUCX_Data[idx_ucx].rem_addr, pServer->pUCX_Data[idx_ucx].rkey, sizeof(int));
    }
    free(pCheckParam->idxList);
    free(pCheckParam);
    // return NULL;
}

void SERVER_RDMA::ScanNewMsg() {
	int i, k, LastQPLocal, idx_queue, idx_ucx, nQP_Server;
	__m512i Data;
	unsigned long int cmpMask, T_Queued;
	struct timeval tm;

	nUCXNewMsg = 0;
	if(p_shm_NewMsgFlag == NULL)	return;
	LastQPLocal = IdxLastQP + 1;

	// pthread_mutex_lock(&process_lock);
	nPreAllUCXNewMsg = nAllUCXNewMsg;
	nAllUCXNewMsg += nUCXNewMsg;
	// pthread_mutex_unlock(&process_lock);
	nUCXNewMsg = 0;

	if(IdxLastQP64 <=192)	{	// simple version
		for(i=0; i<LastQPLocal; i++)	{
			if(p_shm_NewMsgFlag[i])	{
				if(pUCX_Data[i].bServerReady)	{
					UCXNewMsgList[nUCXNewMsg] = i;
	//				printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i);
					nUCXNewMsg++;
					p_shm_NewMsgFlag[i] = 0;
				}
			}
		}
	}
	else	{	// AVX512 version
        for(i=0; i< (IdxLastQP64-64); i+=64)        {
			Data = *( volatile __m512i *)(& p_shm_NewMsgFlag[i]);
			cmpMask = _mm512_movepi8_mask(Data);
			
			if ( cmpMask != 0 ) {
				for(k=0; k<64; k++)	{
					if(cmpMask & 1LL)	{
						if(pUCX_Data[i+k].bServerReady)	{
							UCXNewMsgList[nUCXNewMsg] = i + k;
	//						printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i+k);
							nUCXNewMsg++;
							p_shm_NewMsgFlag[i+k] = 0;
						}
					}
					cmpMask = cmpMask >> 1;
				}
			}
		}
        for(; i< IdxLastQP64; i+=64)        {	// residue
			Data = *( volatile __m512i *)(& p_shm_NewMsgFlag[i]);
			cmpMask = _mm512_movepi8_mask(Data);
			
			if ( cmpMask != 0 ) {
				for(k=0; k<64; k++)	{
					if(cmpMask & 1LL)	{
						if( (i + k) >= LastQPLocal )	{	// reached the end
							break;
						}
						if(pUCX_Data[i+k].bServerReady)	{
							UCXNewMsgList[nUCXNewMsg] = i + k;
	//						printf("DBG> Rank = %d. Found new msg for qp %d.\n", mpi_rank, i+k);
							nUCXNewMsg++;
							p_shm_NewMsgFlag[i+k] = 0;
						}
					}
					cmpMask = cmpMask >> 1;
				}
			}
		}
	}
    
	gettimeofday(&tm, NULL);
	T_Queued = tm.tv_sec * 1000000 + tm.tv_usec;
    CHECKPARAM* pParam = (CHECKPARAM*)malloc(sizeof(CHECKPARAM));
    pParam->pServer = this;
    pParam->idxList = (int*)malloc(sizeof(int) * nUCXNewMsg);
    pParam->size = nUCXNewMsg;
    pParam->p_shm_IO_Cmd_Msg = p_shm_IO_Cmd_Msg;
	for(i=0; i<nUCXNewMsg; i++)	{
		idx_ucx = UCXNewMsgList[i];
		pParam->idxList[i] = idx_ucx;
	}
	// pthread_mutex_lock(&process_lock);
	nAllUCXNewMsg += nUCXNewMsg;
	if(nPreAllUCXNewMsg != nAllUCXNewMsg) {
        fprintf(stdout, "RANK=%d nAllUCXNewMsg %d newMsgThisRound %d\n", mpi_rank, nAllUCXNewMsg, nUCXNewMsg);
    }
    // printf("ScanNewMsg\n");
	// pthread_mutex_unlock(&process_lock);

    // pthread_t pthread_check_data;
    // if(pthread_create(&pthread_check_data, NULL, Func_thread_Check_Data, (void*)pParam)) {
    //     fprintf(stderr, "Error creating thread Func_thread_Check_Data().\n");
    //     return;
    // }
    Func_thread_Check_Data((void*)pParam);

}

void SERVER_RDMA::Drain_Client(const int fd)
{
	int rc, idx, nBytes;
	char buf[3072];
	UCXPARAM *pUCXParam=NULL;
	UCX_DATA_SEND_BY_SERVER *pData_to_send=NULL;
	UCX_DATA_SEND_BY_CLIENT *pData_to_recv=NULL;
	pthread_t pthread_Setup_UCX;
	
	rc = read(fd, buf, sizeof(UCX_DATA_SEND_BY_CLIENT));
	switch(rc) {
        default:
        {
			idx = p_Hash_socket_fd->DictSearchOrg(fd, &elt_list_socket_fd, &ht_table_socket_fd);
            assert(idx >= 0);
            pUCXParam = (UCXPARAM*)malloc( sizeof(UCXPARAM) + sizeof(UCX_DATA_SEND_BY_CLIENT) + sizeof(UCX_DATA_SEND_BY_SERVER) );
            assert(pUCXParam != NULL);
            pData_to_recv = (UCX_DATA_SEND_BY_CLIENT *)((char*)pUCXParam + sizeof(UCXPARAM));
            pData_to_send = (UCX_DATA_SEND_BY_SERVER *)((char*)pUCXParam + sizeof(UCXPARAM) + sizeof(UCX_DATA_SEND_BY_CLIENT) );
            memcpy(pData_to_recv, buf, sizeof(UCX_DATA_SEND_BY_CLIENT));

            server_create_ep(pUCX_Data[idx].ucp_data_worker, (ucp_address_t*)pData_to_recv->ucx.peer_address, &pUCX_Data[idx].peer_ep);
            pUCX_Data[idx].nPut_Get = 0;
            pUCX_Data[idx].nPut_Get_Done = 0;

            ucs_status_t status = ucp_ep_rkey_unpack(pUCX_Data[idx].peer_ep, pData_to_recv->ib_mem.rkey_buffer, &pUCX_Data[idx].rkey);
            assert(status == UCS_OK);
            pUCX_Data[idx].rem_addr = pData_to_recv->ib_mem.addr;


            pData_to_send->ucx.comm_tag = TAG_EXCH_UCX_INFO;
            memcpy(pData_to_send->ucx.peer_address, pUCX_Data[idx].address_p, pUCX_Data[idx].address_length);
            pData_to_send->ucx.peer_address_length = pUCX_Data[idx].address_length;

            pData_to_send->ib_mem.comm_tag = TAG_EXCH_MEM_INFO;
            memcpy(pData_to_send->ib_mem.rkey_buffer, rkey_buffer, rkey_buffer_size);
            pData_to_send->ib_mem.rkey_buffer_size = rkey_buffer_size;
            pData_to_send->ib_mem.addr = (uint64_t)(p_shm_Global);

            pData_to_send->global_addr.comm_tag = TAG_GLOBAL_ADDR_INFO;
            pData_to_send->global_addr.addr_NewMsgFlag = (uint64_t)p_shm_NewMsgFlag + sizeof(char)*idx;
            pData_to_send->global_addr.addr_IO_Cmd_Msg = (uint64_t)p_shm_IO_Cmd_Msg + BLOCK_SIZE*idx;
			// fprintf(stdout, "DBG> SERVER_RDMA Drain Client %d ucx_idx %d addr_IO_Cmd_Msg %p\n", fd, idx, pData_to_send->global_addr.addr_IO_Cmd_Msg);
            nBytes = write(fd, pData_to_send, sizeof(UCX_DATA_SEND_BY_SERVER));
            assert(nBytes == sizeof(UCX_DATA_SEND_BY_SERVER));

            pUCXParam->fd = fd;
            pUCXParam->idx = idx;
            pUCXParam->pServer_UCX = this;

            if(pthread_create(&pthread_Setup_UCX, NULL, Func_thread_Finish_UCX_Setup, (void*)pUCXParam)) {
                fprintf(stderr, "Error creating thread Func_thread_Finish_UCX_Setup().\n");
                return;
            }

            break;
        }
        case 0: 
        {
            idx = p_Hash_socket_fd->DictSearchOrg(fd, &elt_list_socket_fd, &ht_table_socket_fd);
            if(idx >= 0)	{
                p_Hash_socket_fd->DictDelete(fd, &elt_list_socket_fd, &ht_table_socket_fd);	// !!!!!!!!!!!!!!!!!!!!!!!!!! only for test!!!!
                Del_Epoll(fd);
                close(fd);
            }
            else	{
                printf("Error: ucx failed to find fd %d from hash table.\n", fd);
            }
            break;
        }
        case -1: 
        {
            printf("INFO> ucx recv: %s\n", strerror(errno));    break;  
        }
	}
	
	if (rc != 0) return;
}

int SERVER_RDMA::Add_Epoll(int events, int fd)
{
	int rc;
	struct epoll_event ev;
	
	memset(&ev,0,sizeof(ev)); // placate valgrind
	ev.events = events;
	ev.data.fd= fd;
//	printf("INFO> Adding fd %d to epoll\n", fd);
	rc = epoll_ctl(sock_epoll_fd, EPOLL_CTL_ADD, fd, &ev);
	if (rc == -1) {
		fprintf(stderr, "ERROR> epoll_ctl: %s\n", strerror(errno));
	}
	return rc;
}

int SERVER_RDMA::Del_Epoll(int fd)
{
	int rc;
	struct epoll_event ev;
	rc = epoll_ctl(sock_epoll_fd, EPOLL_CTL_DEL, fd, &ev);
	if (rc == -1) {
		fprintf(stderr, "ERROR> epoll_ctl: %s\n", strerror(errno));
	}
	return rc;
}

int SERVER_RDMA::Setup_Listener(void) {
    int rc = -1, one=1;
	
	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd == -1) {
		fprintf(stderr, "ERROR> socket: %s\n", strerror(errno));
	}
	else	{
		// internet socket address structure: our address and port
		struct sockaddr_in sin;
		sin.sin_family = AF_INET;
		sin.sin_addr.s_addr = sock_addr;
		sin.sin_port = htons(sock_port);
		
		// bind socket to address and port 
		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		if (bind(fd, (struct sockaddr*)&sin, sizeof(sin)) == -1) {
			fprintf(stderr, "ERROR> bind: %s\n", strerror(errno));
		}
		else	{
			// put socket into listening state
			if (listen(fd,1600) == -1) {
				fprintf(stderr, "listen: %s\n", strerror(errno));
			}
			else	{
				sock_fd = fd;
				rc=0;
			}
		}
	}
	
	if ((rc < 0) && (fd != -1)) close(fd);
	return rc;
}

ucs_status_t SERVER_RDMA::server_create_ep(ucp_worker_h data_worker,
                                     ucp_conn_request_h conn_request,
                                     ucp_ep_h *server_ep)
{
    ucp_ep_params_t ep_params;
    ucs_status_t    status;

    /* Server creates an ep to the client on the data worker.
     * This is not the worker the listener was created on.
     * The client side should have initiated the connection, leading
     * to this ep's creation */
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                UCP_EP_PARAM_FIELD_CONN_REQUEST;
    ep_params.conn_request    = conn_request;
    ep_params.err_handler.cb  = err_cb;
    ep_params.err_handler.arg = NULL;

    status = ucp_ep_create(data_worker, &ep_params, server_ep);
    if (status != UCS_OK) {
        fprintf(stderr, "mpi_rank %d failed to create an endpoint on the server: (%s)\n", mpi_rank,
                ucs_status_string(status));
    }

    return status;
}

int SERVER_RDMA::Get_IO_Worker_Index_from_UCX_Index(int idx_ucx)
{
	int nNumQueuePerWorker, idx_Queue, nUCX_InterServer;

	nUCX_InterServer = nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	if(idx_ucx < nUCX_InterServer)	return (idx_ucx % NUM_THREAD_IO_WORKER_INTER_SERVER);

//	idx_Queue = ( ((idx_ucx-NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx_ucx-NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_M1 ) % MAX_NUM_QUEUE_M1) + NUM_THREAD_IO_WORKER_INTER_SERVER;
//	idx_Queue = ( ((idx_ucx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER + (idx_ucx-nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + nFSServer*NUM_THREAD_IO_WORKER_INTER_SERVER;
	idx_Queue = ( ((idx_ucx - nUCX_InterServer)*NUM_QUEUE_PER_WORKER + (idx_ucx - nUCX_InterServer)*NUM_QUEUE_PER_WORKER/MAX_NUM_QUEUE_MX ) % MAX_NUM_QUEUE_MX) + NUM_THREAD_IO_WORKER_INTER_SERVER;

	if( ( ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) % ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ) == 0 )	{
		nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) ;
	}
	else	{
		nNumQueuePerWorker = ( MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER ) / ( NUM_THREAD_IO_WORKER - NUM_THREAD_IO_WORKER_INTER_SERVER ) + 1;
	}

//	if(idx_ucx < nUCX_InterServer)	{	// inter-server qp
//		return pQP_Data[idx_ucx].idx_queue;
//	}
//	else	{
		return (( (idx_Queue-NUM_THREAD_IO_WORKER_INTER_SERVER)/nNumQueuePerWorker ) + NUM_THREAD_IO_WORKER_INTER_SERVER);
//	}
}

void SERVER_RDMA::AllocateUCPDataWorker(int idx) {
    UCX_DATA *pUCX;
	int idx_io_worker;
	
	pUCX = &(pUCX_Data[idx]);
    pUCX_Data[idx].bServerReady = 0;
	idx_io_worker = Get_IO_Worker_Index_from_UCX_Index(idx);

    pUCX->ucp_data_worker = ucp_data_worker[idx_io_worker];

    if(pthread_mutex_init(&(pUCX->ucx_lock), NULL) != 0) {
		perror("pthread_mutex_init");
		exit(1);
	}

    ucs_status_t status;
    status = ucp_worker_get_address(pUCX->ucp_data_worker, &pUCX->address_p, &pUCX->address_length);
    assert(pUCX->address_length <= MAX_UCP_ADDR_LEN);
    if(status != UCS_OK) {
        fprintf(stderr, "AllocateUCPDataWorker Failure: %s.\n", ucs_status_string(status));
		exit(1);
    } 
    else {
        fprintf(stdout, "mpi_rank %d idx %d idx_io_worker %d: AllocateUCPDataWorker Success: %s %d\n", mpi_rank, idx, idx_io_worker, pUCX->address_p, pUCX->address_length);
    }
}

int SERVER_RDMA::Init_Context(ucp_context_h *ucp_context, ucp_worker_h *ucp_worker)
{
	/* UCP objects */
    ucp_params_t ucp_params;
    ucs_status_t status;
    int ret = 0;

    memset(&ucp_params, 0, sizeof(ucp_params));

    /* UCP initialization */
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_NAME | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    ucp_params.name       = "ucp_themisio_server";
    ucp_params.mt_workers_shared = 1;
    ucp_params.features = UCP_FEATURE_RMA;
    status = ucp_init(&ucp_params, NULL, ucp_context);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_init (%s)\n", ucs_status_string(status));
        ret = -1;
        goto err;
    }
    ret = Init_Worker(*ucp_context, ucp_worker);
    if (ret != 0) {
        goto err_cleanup;
    }

    return ret;
err_cleanup:
    ucp_cleanup(*ucp_context);
err:
    return ret;
}

int SERVER_RDMA::Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker) {
    ucp_worker_params_t worker_params;
    ucs_status_t status;
    int ret = 0;

    memset(&worker_params, 0, sizeof(worker_params));

    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

    status = ucp_worker_create(ucp_context, &worker_params, ucp_worker);
    if (status != UCS_OK) {
        fprintf(stderr, "failed to ucp_worker_create (%s)\n", ucs_status_string(status));
        ret = -1;
    }
    return ret;
}

void SERVER_RDMA::Init_Server_Memory(int max_num_qp, int port) {
    printf("Rank = %d. Init_Server_Memory.\n", mpi_rank);
    int i, nSizeofNewMsgFlag, nSizeofHeartBeat, nSizeofIOCmdMsg, nSizeofIOResult, nSizeofIOResult_Recv, nSizePerCallReturnBlock, nSizeofReturnBuffer;
	int offset;
	

    sock_addr = INADDR_ANY;
	sock_fd = -1;
	sock_signal_fd = -1;
	sock_epoll_fd = -1;
	sock_port = port;

    max_qp = max_num_qp;
	
    p_Hash_socket_fd = (CHASHTABLE_INT *)malloc(CHASHTABLE_INT::GetStorageSize(max_num_qp*2));
	p_Hash_socket_fd->DictCreate(max_num_qp*2, &elt_list_socket_fd, &ht_table_socket_fd);

    pUCX_Data = (UCX_DATA *)malloc(sizeof(UCX_DATA) * max_num_qp);
    assert(pUCX_Data != NULL);
    for(i=0; i<max_qp; i++)	{
		pUCX_Data[i].ucp_data_worker = NULL;
	}

    nSizeofNewMsgFlag = sizeof(char)*max_qp;
	nSizeofIOCmdMsg = BLOCK_SIZE*max_qp;
	
	
	nSizeshm_Global = nSizeofNewMsgFlag + nSizeofIOCmdMsg;
	p_shm_Global = memalign( 4096, nSizeshm_Global);
	assert(p_shm_Global != NULL);
	memset(p_shm_Global, 0, nSizeshm_Global);

	offset = 0;
	p_shm_NewMsgFlag = (unsigned char *)p_shm_Global;
	offset += nSizeofNewMsgFlag;

	p_shm_IO_Cmd_Msg = (unsigned char *)((char*)p_shm_Global + offset);
	offset += nSizeofIOCmdMsg;

	

    FirstAV_QP = 0;
	nQP = 0;
	IdxLastQP = -1;
	IdxLastQP64 = -1;

    ucs_status_t status;
    status = RegisterBuf_RW_Local_Remote(p_shm_Global, nSizeshm_Global, &mr_shm_global);
	assert(mr_shm_global != NULL);
    assert(status == UCS_OK);

    rem_buff = (unsigned char*)p_shm_Global;
    rem_buff_size = nSizeshm_Global;
	status = ucp_rkey_pack(ucp_main_context, mr_shm_global, &rkey_buffer, &rkey_buffer_size);
    assert(rkey_buffer != NULL);
    assert(rkey_buffer_size <= MAX_UCP_RKEY_SIZE);
    assert(status == UCS_OK);
	fprintf(stdout, "DBG> Init_Server_Memory successfully\n");
}

ucs_status_t SERVER_RDMA::RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh) {
    uct_allocated_memory_t alloc_mem;
    ucp_mem_map_params_t mem_map_params;
    mem_map_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mem_map_params.length = len;
    mem_map_params.address = buf;
    ucs_status_t status = ucp_mem_map(ucp_main_context, &mem_map_params, memh);
	if(memh == NULL) {
		perror("ucp_mem_map");
		fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_mem_map on RW_Local_Remote.\n", __FILE__, __LINE__);
		char szHostName[128];
		gethostname(szHostName, 63);
		printf("DBG> Hostname = %s pid = %d\n", szHostName, getpid());
		fflush(stdout);
		sleep(300);
		exit(1);
	}
	nSizeUCXReg += len;
    return status;
}

void SERVER_RDMA::UCX_Pack_Rkey(ucp_mem_h memh, void *rkey_buffer) {
	void* tmp_rkey_buffer;
	size_t rkey_buffer_size;
	ucs_status_t status = ucp_rkey_pack(ucp_main_context, memh, &tmp_rkey_buffer, &rkey_buffer_size);
	if(status != UCS_OK) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_rkey_pack in UCX_Unpack_Rkey(). nConnectionAccu = %d\n", 
			__FILE__, __LINE__, nConnectionAccu);
		exit(1);
	}
	memcpy(rkey_buffer, tmp_rkey_buffer, rkey_buffer_size);
	ucp_rkey_buffer_release(tmp_rkey_buffer);
}

void SERVER_RDMA::UCX_Unpack_Rkey(int idx, void* rkey_buffer, ucp_rkey_h* rkey_p) {
	ucs_status_t status = ucp_ep_rkey_unpack(pUCX_Data[idx].peer_ep, rkey_buffer, rkey_p);
	if(status != UCS_OK) {
		fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_ep_rkey_unpack in UCX_Unpack_Rkey().n", 
			__FILE__, __LINE__);
		exit(1);
	}
}

void SERVER_RDMA::WorkerProgress() {
   for(int i=0; i<NUM_THREAD_IO_WORKER; i++)	{
		assert(ucp_data_worker[i] != NULL);
        ucp_worker_progress(ucp_data_worker[i]);
	}
}

void SERVER_RDMA::UCX_Put(int idx, void* loc_buf, void* rem_buf, ucp_rkey_h rkey, size_t len) {
    long int t1_ms, t2_ms;
    struct timeval tm1, tm2;
    int bTimeOut=0;	// the flag of time out in PUT. 
    if(pUCX_Data[idx].bTimeout)	{	// Something wrong with this QP. Client may disconnect or die...
		printf("WARNING> UCX %d got timeout in previous Put(). Ignore all OPs for this UCX.\n", 
			idx);
		return;
	}
    ucp_ep_h peer_ep = pUCX_Data[idx].peer_ep;
    ucp_worker_h ucp_data_worker = pUCX_Data[idx].ucp_data_worker;
retry:
    ucp_request_param_t param;
    memset(&param, 0, sizeof(ucp_request_param_t));
    param.op_attr_mask = 0;
    ucs_status_ptr_t req = ucp_put_nbx(peer_ep, loc_buf, len, (uint64_t)rem_buf, rkey, &param);
    // ucs_status_t status = ucp_request_check_status(req);
    if(UCS_PTR_IS_ERR(req)) {
        if(bTimeOut) {
            printf("ERROR> Rank = %d Put Timeout in UCX(%d) Put %zu bytes\n", 
				mpi_rank, idx, len);
            return;
        }
        fprintf(stderr, "Error occured at %s:L%d. Failure: ucp_put_nbx in Put().\n", 
			__FILE__, __LINE__);
		exit(1);
    }
    pUCX_Data[idx].nPut_Get++;
	if(req == NULL) {
		pUCX_Data[idx].nPut_Get_Done += 1;
		// fprintf(stdout, "DBG> UCX_Put returns immediately loc %p rem %p\n", loc_buf, rem_buf);
	}
    if( (pUCX_Data[idx].nPut_Get - pUCX_Data[idx].nPut_Get_Done) >= UCX_QUEUE_SIZE ) {
        gettimeofday(&tm1, NULL);
		t1_ms = (tm1.tv_sec * 1000) + (tm1.tv_usec / 1000);
        while(1) {
            // fprintf(stdout, "ucp_worker_progress\n");
            ucp_worker_progress(ucp_data_worker);
            gettimeofday(&tm2, NULL);
			t2_ms = (tm2.tv_sec * 1000) + (tm2.tv_usec / 1000);
            ucs_status_t status = ucp_request_check_status(req);
            if(status == UCS_OK) {
                pUCX_Data[idx].nPut_Get_Done +=1;
				// fprintf(stdout, "DBG> UCX_Put UCS_OK loc %p rem %p\n", loc_buf, rem_buf);
                break;
            }
            else if(status == UCS_INPROGRESS) {
                if( (t2_ms - t1_ms) > UCX_PUT_TIMEOUT_MS )	{
					bTimeOut = 1;
					goto retry;
				}
            }
            else {
                fprintf(stderr, "ucp_put_nbx failed %s\n", ucs_status_string(status));
//				pthread_mutex_unlock(&(pUCX_Data[idx].ucx_lock));
				exit(1);
				return;
            }
        }
    }
    if(req != NULL) ucp_request_free(req);
}


