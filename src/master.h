#pragma once

#include <bits/stdc++.h>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

using namespace std;

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using masterworker::WorkerService;
using masterworker::WorkerQuery;
using masterworker::WorkerShard;
using masterworker::WorkerResponse;

static int debug_level = 2;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {
public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec&, const std::vector<FileShard>&);

	/* DON'T change this function's signature */
	bool run();

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	MapReduceSpec mr_spec_;
	vector<FileShard> file_shards_;
	vector<bool> shard_assigned;

	int n_shards;
	int n_workers;
	int n_output_files;

	unordered_map<string, FileShard> shard_mapping;
	unordered_map<string, unique_ptr < WorkerService::Stub > > stub_mapping;
	unordered_map<int, vector<FileInfo>> id_to_shards;
	unordered_map<int, vector<int>> worker_id_to_reducer_id;

	bool map();
	bool reduce();
	int parse_tag(void* tag);
};


int Master::parse_tag(void* tag) {
	for(int i = 0; i < n_workers; i++) {
		if(tag == (void*)i) {
			return i;
		}
	}
	return -1;
}

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const vector<FileShard>& file_shards): shard_assigned(file_shards.size(), false) {
	mr_spec_ = mr_spec;
	file_shards_ = file_shards;

	n_shards = file_shards.size();
	n_workers = mr_spec_.n_workers;
	n_output_files = mr_spec_.n_output_files;

	// create stubs
	for(auto workerIP : mr_spec.worker_IPs){
		shared_ptr < Channel > channel = grpc::CreateChannel(workerIP, grpc::InsecureChannelCredentials());
		stub_mapping[workerIP] = WorkerService::NewStub(channel);
	}

	// print elements of stub_mapping
	for(auto it = stub_mapping.begin(); it != stub_mapping.end(); it++){
		cout << it->first << endl;
	}

	system("rm -rf intermediate");
	system("mkdir intermediate");

	string command1 = "rm -rf " + mr_spec_.output_dir;
	string command2 = "mkdir " + mr_spec_.output_dir;

	system(command1.c_str());
	system(command2.c_str());

}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	return map() && reduce();	
}

bool Master::map(){
	if(debug_level > 1)
		cout << "Map start" << endl;

	if(debug_level > 1) {
		cout << "n_shards:" << n_shards << endl;
		cout << "n_workers:" << n_workers << endl;
	}

	int worker_pointer = 0;
	for(int i = 0; i < n_shards; i++) {
		for(int j = 0; j < file_shards_[i].file_info.size(); j++) {
			if(id_to_shards.find(worker_pointer) == id_to_shards.end()) {
				id_to_shards[worker_pointer] = vector<FileInfo>();
			}
			id_to_shards[worker_pointer].push_back(file_shards_[i].file_info[j]);
		}
		worker_pointer = (worker_pointer + 1) % n_workers;
	}

	if(debug_level > 1)
		cout << "Master map starting rpc" << endl;
	
	// The producer-consumer queue we use to communicate asynchronously with the
	// gRPC runtime.
	CompletionQueue cq;

	// Container for the data we expect from the server.
	WorkerResponse reply;
	
	// Storage for the status of the RPC upon completion.
	vector<Status> status_array(n_workers);

	for(auto it = id_to_shards.begin(); it != id_to_shards.end(); it++) {
		if(debug_level > 1)
			cout << "Master run map start " << it->first << endl;
		
		ClientContext context;
		context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(15000));

		WorkerQuery worker_query;
		worker_query.set_type("MAP");
		worker_query.set_userid(mr_spec_.user_id);
		worker_query.set_n(n_output_files);
		worker_query.set_output(mr_spec_.output_dir);
		worker_query.set_workerid(it->first);

		if(debug_level > 1)
			cout << "Master run map created query " << it->first << endl;

		for(auto file : it->second) {
			WorkerShard* worker_shard = worker_query.add_shards();
			worker_shard->set_path(file.filename);
			worker_shard->set_start(file.start_offset);
			worker_shard->set_end(file.end_offset);
		}

		if(debug_level > 1)
			cout << "Master run map starting rpc " << it->first << endl;

		// stub_->PrepareAsyncSayHello() creates an RPC object, returning
		// an instance to store in "call" but does not actually start the RPC
		// Because we are using the asynchronous API, we need to hold on to
		// the "call" instance in order to get updates on the ongoing RPC.
		std::unique_ptr < ClientAsyncResponseReader < WorkerResponse > > rpc(
			stub_mapping[mr_spec_.worker_IPs[it->first]] -> PrepareAsyncassignTask( & context, worker_query, & cq));

		// StartCall initiates the RPC call
		rpc -> StartCall();

		// Request that, upon completion of the RPC, "reply" be updated with the
		// server's response; "status" with the indication of whether the operation
		// was successful. Tag the request with the integer 1.
		rpc -> Finish( & reply, & status_array[it->first], (void * ) it->first);

		if(debug_level > 1)
			cout << "Master run map wait " << it->first << endl;
	}
	
	// Something store here
	vector<bool> returned(mr_spec_.worker_IPs.size(), false);

	for(auto it = id_to_shards.begin(); it != id_to_shards.end(); it++){
		if(debug_level > 1)
			cout << "Master run map reply wait " << it->first << endl;

		void* got_tag;
		bool ok = false;
		// Block until the next result is available in the completion queue "cq".
		// The return value of Next should always be checked. This return value
		// tells us whether there is any kind of event or the cq_ is shutting down.
		GPR_ASSERT(cq.Next(&got_tag, &ok));

		// Verify that the result from "cq" corresponds, by its tag, our previous
		// request.
		// GPR_ASSERT(got_tag == (void * ) 1);
		GPR_ASSERT(ok);

		if(!status_array[parse_tag(got_tag)].ok()){
			cout << "Master run map RPC Failed " << parse_tag(got_tag) << endl;
		}

		returned[reply.id()] = true;

		if(debug_level > 1)
			cout << "Master run map got " << reply.id() << endl;
	}

	return true;
}

bool Master::reduce(){
	if(debug_level > 1)
		cout << "Reduce start" << endl;

	int worker_pointer = 0;
	for(int i = 0; i < n_output_files; i++) {
		if(worker_id_to_reducer_id.find(worker_pointer) == worker_id_to_reducer_id.end())
			worker_id_to_reducer_id[worker_pointer] = vector<int>();

		worker_id_to_reducer_id[worker_pointer].push_back(i);
		worker_pointer = (worker_pointer + 1) % n_workers;
	}

	if(debug_level > 1)
		cout << "Master reduce starting rpc" << endl;
	
	// The producer-consumer queue we use to communicate asynchronously with the
	// gRPC runtime.
	CompletionQueue cq;

	// Container for the data we expect from the server.
	WorkerResponse reply;
	
	// Storage for the status of the RPC upon completion.
	vector<Status> status_array(n_workers);

	for(auto it = worker_id_to_reducer_id.begin(); it != worker_id_to_reducer_id.end(); it++){
		int i = it->first;

		if(debug_level > 1)
			cout << "Master reduce map start " << i << endl;
		
		ClientContext context;
		context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(15000));

		WorkerQuery worker_query;
		worker_query.set_type("REDUCE");
		worker_query.set_userid(mr_spec_.user_id);
		worker_query.set_n(n_output_files);
		worker_query.set_output(mr_spec_.output_dir);
		worker_query.set_workerid(i);
		
		for(auto reducer_id : it->second)
			worker_query.add_reducerids(reducer_id);

		if(debug_level > 1)
			cout << "Master run reduce created query " << i << endl;

		for(int i = 0; i < n_workers; i++)
			worker_query.add_succeededids(i);

		if(debug_level > 1)
			cout << "Master run reduce starting rpc " << i << endl;

		// stub_->PrepareAsyncSayHello() creates an RPC object, returning
		// an instance to store in "call" but does not actually start the RPC
		// Because we are using the asynchronous API, we need to hold on to
		// the "call" instance in order to get updates on the ongoing RPC.
		std::unique_ptr < ClientAsyncResponseReader < WorkerResponse > > rpc(
			stub_mapping[mr_spec_.worker_IPs[i]] -> PrepareAsyncassignTask( & context, worker_query, & cq));

		// StartCall initiates the RPC call
		rpc -> StartCall();

		// Request that, upon completion of the RPC, "reply" be updated with the
		// server's response; "status" with the indication of whether the operation
		// was successful. Tag the request with the integer 1.
		rpc -> Finish( & reply, & status_array[i], (void * ) i);

		if(debug_level > 1)
			cout << "Master run reduce wait " << i << endl;
	}
	
	// Something store here
	vector<bool> returned(mr_spec_.worker_IPs.size(), false);

	for(auto it = id_to_shards.begin(); it != id_to_shards.end(); it++) {
		int i = it->first;

		if(debug_level > 1)
			cout << "Master run reduce reply wait " << i << endl;

		void* got_tag;
		bool ok = false;
		// Block until the next result is available in the completion queue "cq".
		// The return value of Next should always be checked. This return value
		// tells us whether there is any kind of event or the cq_ is shutting down.
		GPR_ASSERT(cq.Next(&got_tag, &ok));

		// Verify that the result from "cq" corresponds, by its tag, our previous
		// request.
		// GPR_ASSERT(got_tag == (void * ) 1);
		GPR_ASSERT(ok);

		if(!status_array[parse_tag(got_tag)].ok()){
			cout << "Master run reduce RPC Failed " << parse_tag(got_tag) << endl;
		}

		returned[reply.id()] = true;

		if(debug_level > 1)
			cout << "Master run reduce got " << reply.id() << endl;
	}

	return true;
}
