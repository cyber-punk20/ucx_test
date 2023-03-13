CXX=g++
OPT=-O2 -g
# OPT=
CXXFLAGS=-Iinclude/ -march=skylake-avx512 -g -I/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/include -I$(UCX_INCLUDE_DIRECTORY)  -DNCX_PTR_SIZE=8 -pipe -DLOG_LEVEL=4  -DPAGE_MERGE
OBJS=obj/server.o obj/dict.o obj/xxhash.o obj/ucx_rma.o
#HEADERS=dict.h qp_common.h qp.h io_queue.h utility.h xxhash.h list.h buddy.h myfs_common.h myfs.h io_ops_common.h io_ops.h ncx_slab.h ncx_core.h ncx_log.h client/qp_client.h
HEADERS=dict.h utility.h xxhash.h ucx_rma.h common_info.h ucx_client.h
RM=rm -rf

# in cmd of windows
ifeq ($(SHELL),sh.exe)
    RM := del /f/q
endif

#all: server fsclient
all: server client

#iops_stat_mpi:
#	mpicc -g -O0 -o tests/iops_stat_mpi tests/iops_stat_mpi.c

server: $(OBJS)
	$(CXX) -O2 $(CXXFLAGS) -g -o $@ obj/server.o obj/ucx_rma.o obj/dict.o obj/xxhash.o -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib/release_mt -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib -L$(UCX_LIB_DIRECTORY) -lmpicxx -lmpifort -lmpi -ldl -lucp -lucm -lucs -luct
# $(CXX) -O2 $(CXXFLAGS) -g -o $@ obj/server.o obj/ucx_rma.o obj/dict.o obj/xxhash.o -libverbs -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib/release_mt -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib -L$(UCX_LIB_DIRECTORY) -lmpicxx -lmpifort -lmpi -ldl -lucp -lucm -lucs -luct


client: $(OBJS)
	$(CXX) -O2 $(CXXFLAGS) -g -o $@ obj/client.o obj/ucx_client.o -lpthread -lrt -Wunused-variable -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib/release_mt -L/opt/intel/compilers_and_libraries_2018.6.288/linux/mpi/intel64/lib -L$(UCX_LIB_DIRECTORY) -lmpicxx -lmpifort -lmpi -ldl -lucp -lucm -lucs -luct

#fsclient: $(OBJS)
#	$(CXX) $(CXXFLAGS) -o $@ put_get_client.o dict.o xxhash.o -libverbs -lpthread -lrt

# disable optimization. Possible threading bugs.
obj/server.o: server.cpp $(HEADERS)
	$(CXX) -O0 $(CXXFLAGS) -c -o obj/server.o $<


obj/dict.o: dict.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/dict.o $<

obj/xxhash.o: xxhash.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/xxhash.o $<

obj/ucx_rma.o: ucx_rma.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/ucx_rma.o  $<

obj/ucx_client.o: ucx_client.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/ucx_client.o  $<

obj/client.o: client.cpp $(HEADERS)
	$(CXX) $(OPT) $(CXXFLAGS) -c -o obj/client.o  $<

#put_get_client.o: client/put_get_client.cpp $(HEADERS)
#	$(CXX) $(CXXFLAGS) -c $<

#run: myfs
#	./myfs
#	dot bdgraph.dot -Tpng > bd.png

clean:
	$(RM) obj/*.o 
#	$(RM) *.o server fsclient
