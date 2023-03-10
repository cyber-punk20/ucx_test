#ifndef __UCX_CLIENT
#define __UCX_CLIENT
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <assert.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <signal.h>
#include <sys/syscall.h>
#include<signal.h>
#include <sys/time.h>
#include <malloc.h>
#include <netinet/tcp.h>

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>

#include "utility.h"
#include "dict.h"

#include "common_info.h"

typedef	struct	{
	char szIP[16];
	int port;
	int nQP;	// number of active queue pairs on this node
	pthread_mutex_t fs_qp_lock;	// 40 bytes
}FS_SEVER;

typedef	struct	{
	int Init_Start;
	int Init_Done;
	int nFSServer;
	int nNUMAPerNode;
	int myip;
	int pad[3];
	FS_SEVER FS_List[MAX_FS_UCX_SERVER];
}FSSERVERLIST;

class CLIENT_UCX {
private:
	UCX_EXCH_DATA ucx_my_data, ucx_pal_data;
	UCX_IB_MEM_DATA my_remote_mem;
	uint64_t nPut=0, nPut_Done=0;
	uint64_t nGet=0, nGet_Done=0;
	pthread_mutex_t ucx_put_get_lock;
	int sock = 0;

	void server_create_ep(void);
	void AllocateUCPDataWorker(void);
	void Setup_Socket(char szServerIP[]);

	static int Init_Context(ucp_context_h *ucp_context);
	int Init_Worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);

public:
	static ucp_context_h ucp_main_context;
	static int Done_UCX_Init;
	static void Init_UCX_Env(void);

	ucp_ep_h server_ep;
	UCX_IB_MEM_DATA pal_remote_mem;
	
	ucp_worker_h  ucp_worker = NULL;

	void* ucx_rem_buff;
    ucp_mem_h ucx_mr_rem = NULL;

	uint64_t remote_addr_new_msg;	// the address of remote buffer to notify a new message
	uint64_t remote_addr_IO_CMD;	// the address of remote buffer to IO requests.
	int ucx_put_get_locked;
	int tid = 0;
	int Idx_fs = -1;

	void CloseUCPDataWorker(void);
	void Setup_UCP_Connection(int IdxServer);
	static ucs_status_t RegisterBuf_RW_Local_Remote(void* buf, size_t len, ucp_mem_h* memh);
	int UCX_Put(void* loc_buff, void* rem_buf, ucp_rkey_h rkey, size_t len);
    int UCX_Get(void* loc_buff, void* rem_buf, ucp_rkey_h rkey, size_t len);
	static void UCX_Pack_Rkey(ucp_mem_h memh, void *rkey_buffer);
};




#endif