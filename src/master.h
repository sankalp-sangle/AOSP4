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

	unordered_map<string, FileShard> shard_mapping;
	unordered_map<string, unique_ptr < WorkerService::Stub > > stub_mapping;

	bool map();
	bool reduce();
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const vector<FileShard>& file_shards): shard_assigned(file_shards.size(), false) {
	mr_spec_ = mr_spec;
	file_shards_ = file_shards;

	n_shards = file_shards.size();
	n_workers = file_shards.size();

	// create stubs
	for(auto workerIP : mr_spec.worker_IPs){
		shared_ptr < Channel > channel = grpc::CreateChannel(workerIP, grpc::InsecureChannelCredentials());
		stub_mapping[workerIP] = WorkerService::NewStub(channel);
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	return map() && reduce();	
}

bool Master::map(){
	if(debug_level > 1)
		cout << "Map start" << endl;

	// Data we are sending to the server.
	WorkerQuery query;
	query.set_userid("cs6210");
	query.set_type("MAP");

	// Container for the data we expect from the server.
	WorkerResponse reply;

	// The producer-consumer queue we use to communicate asynchronously with the
	// gRPC runtime.
	CompletionQueue cq;

	// Storage for the status of the RPC upon completion.
	Status status;

	if(debug_level > 1)
		cout << "Map run start" << endl;

	// Since a stub is of type unique pointer, no copy can be created for this.
	// Hence we cannot use index based iterator or every auto based iterators
	// The only solution here is to use explicit iterators

	// Context for the client. It could be used to convey extra information to
	// the server and/or tweak certain RPC behaviors.
	ClientContext context;

	if(debug_level > 1)
		cout << "Master run rpc start" << endl;

	// stub_->PrepareAsyncSayHello() creates an RPC object, returning
	// an instance to store in "call" but does not actually start the RPC
	// Because we are using the asynchronous API, we need to hold on to
	// the "call" instance in order to get updates on the ongoing RPC.
	std::unique_ptr < ClientAsyncResponseReader < WorkerResponse > > rpc(
		stub_mapping["localhost:50051"] -> PrepareAsyncassignTask( & context, query, & cq));

	// StartCall initiates the RPC call
	rpc -> StartCall();

	// Request that, upon completion of the RPC, "reply" be updated with the
	// server's response; "status" with the indication of whether the operation
	// was successful. Tag the request with the integer 1.
	rpc -> Finish( & reply, & status, (void * ) 1);

	void* got_tag;
	bool ok = false;
	// Block until the next result is available in the completion queue "cq".
	// The return value of Next should always be checked. This return value
	// tells us whether there is any kind of event or the cq_ is shutting down.
	GPR_ASSERT(cq.Next(&got_tag, &ok));

	// Verify that the result from "cq" corresponds, by its tag, our previous
	// request.
	GPR_ASSERT(got_tag == (void * ) 1);
	GPR_ASSERT(ok);

	// Act upon the status of the actual RPC.
	if (status.ok()) {
		// Success
		if(debug_level > 1)
			cout << "Master run rpc success" << endl;
		return true;
	} else {
		// Error
		if(debug_level > 1)
			cout << "Master run rpc error" << endl;
		return false;
	}
}

bool Master::reduce(){
	return true;
}
