
#ifndef __COMMON_INFO_H
#define __COMMON_INFO_H


#define BLOCK_SIZE (1024 * 1024 * 5)
#define UCX_FS_PARAM_FILE "ucx_myfs.param"
#define MAX_UCP_RKEY_SIZE (150)
#define MAX_UCP_ADDR_LEN (450)
#define MAX_FS_UCX_SERVER (128)

#define IB_DEVICE	"ib0"
#define DEFAULT_REM_BUFF_SIZE	(4096)

#define UCX_PUT_TIMEOUT_MS	(3000)	// two seconds timeout
#define UCX_WAIT_RESULT_TIMEOUT_MS	(3500)	// one second timeout

#define NUM_THREAD_IO_WORKER_INTER_SERVER  (8)
#define NUM_THREAD_IO_WORKER  (16+NUM_THREAD_IO_WORKER_INTER_SERVER)

#define MAX_NUM_QUEUE (640 + NUM_THREAD_IO_WORKER_INTER_SERVER)
#define MAX_NUM_QUEUE_MX	(MAX_NUM_QUEUE - NUM_THREAD_IO_WORKER_INTER_SERVER)
#define NUM_QUEUE_PER_WORKER	(MAX_NUM_QUEUE_MX/(NUM_THREAD_IO_WORKER-NUM_THREAD_IO_WORKER_INTER_SERVER))

#define UCX_QUEUE_SIZE	(1)


#define TAG_SUBMIT_JOB_INFO	(0x78000000)
#define TAG_EXCH_UCX_INFO	(0x78780000)
#define TAG_EXCH_MEM_INFO	(0x78780001)
#define TAG_GLOBAL_ADDR_INFO	(0x78780002)
#define TAG_DONE			(0x78787878)


#define TAG_NEW_REQUEST	(0x80)

#define MAX_ENTRY_NAME_LEN    (40)

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include <uct/api/uct.h>

inline int Align64_Int(int a)
{
	// return ( (a & 0x3F) ? (64 + (a & 0xFFFFFFC0) ) : (a) );

	// branch not needed
	return (a + 63) & ~63;
}

inline bool check_test_string(char * str, int size) {
    int i;
    int start = *((int*)str);
    for (i = 4; i < (size - 1); ++i) {
        if(str[i] != 'A' + ((start + i) % 26)) return false;
    }
    return true;
}

inline int generate_test_string(char *str, int size)
{
    int i;
    int start = *((int*)str);
    for (i = 4; i < (size - 1); ++i) {
        str[i] = 'A' + ((start + i) % 26);
    }
    str[i] = '\0';
    return 0;
}

typedef	struct	{
    uint32_t comm_tag;
	char rkey_buffer[MAX_UCP_RKEY_SIZE];
    size_t rkey_buffer_size = 0;
    unsigned long int addr;

	ucp_rkey_h rkey; // only for pal_remote_mem on clients
}UCX_IB_MEM_DATA, *PUCX_IB_MEM_DATA;

typedef	struct	{
    uint32_t comm_tag;
	char peer_address[MAX_UCP_ADDR_LEN];
	size_t peer_address_length = 0;
}UCX_EXCH_DATA;


typedef	struct	{
	uint32_t comm_tag;
	uint64_t addr_NewMsgFlag;
	uint64_t addr_IO_Cmd_Msg;
}UCX_GLOBAL_ADDR_DATA;

typedef	struct{
	UCX_EXCH_DATA	ucx;
	UCX_IB_MEM_DATA		ib_mem;
}UCX_DATA_SEND_BY_CLIENT;

typedef	struct{
	UCX_EXCH_DATA	ucx;
	UCX_IB_MEM_DATA		ib_mem;
	UCX_GLOBAL_ADDR_DATA global_addr;
}UCX_DATA_SEND_BY_SERVER;

#endif