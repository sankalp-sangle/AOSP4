#pragma once

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>

#include <mr_task_factory.h>
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

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {
public:
	/* DON'T change the function signature of this constructor */
	Worker(std::string ip_addr_port);

	/* DON'T change this function's signature */
	bool run();

	~Worker();

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	string server_address;
	WorkerService::AsyncService service_;

	unique_ptr <ServerCompletionQueue> cq_;
	unique_ptr <Server> server_;

	// Class encompasing the state and logic needed to serve a request.
	class CallData {
    public:
        // Take in the "service" instance (in this case representing an asynchronous
        // server) and the completion queue "cq" used for asynchronous communication
        // with the gRPC runtime.
        CallData(WorkerService::AsyncService* service, ServerCompletionQueue* cq, string server_address)
			: service_(service), cq_(cq), responder_( & ctx_), status_(CREATE), server_address(server_address) {
            // Invoke the serving logic right away.
            Proceed();
        }

		void handleMap(){
			if(debug_level > 1)
				cout << server_address << " " << "HandleMap start" << endl;

			// Get the mapper first
			auto mapper = get_mapper_from_task_factory(request_.userid());
			mapper->impl_->n_output_files = request_.n();
			mapper->impl_->output_dir = request_.output();

			if(debug_level > 1)
				cout << server_address << " " << "HandleMap mapper received" << endl;

			int id = request_.workerid();

			for(int i = 0; i < request_.shards().size(); i++) {
				if(debug_level > 1)
					cout << server_address << " " << "HandleMap shard start" << endl;

				WorkerShard shard = request_.shards(i);

				string path = shard.path();
				int start = shard.start();
				int end = shard.end();

				if(debug_level > 1)
					cout << server_address << " " << "HandleMap shard " << path << " " << start << " " << end << endl;

				ifstream file(path, ios::in);

				if(!file.is_open())
					cout << server_address << " " << "HandleMap cant open file" << endl;
				else {
					file.seekg(start, file.beg);

					string tmp;
					while(file.tellg() <= end && file.tellg() != -1) {
						getline(file, tmp);
						mapper->map(tmp);
					}
				}

				file.close();

				if(debug_level > 1)
					cout << server_address << " " << "HandleMap shard end" << endl;
			}

			response_.set_status(1);
			response_.set_id(id);

			// Still have to write stuff to file

			if(debug_level > 1)
				cout << server_address << " " << "HandleMap end" << endl;
		}

		void handleReduce(){
			if(debug_level > 1)
				cout << server_address << " " << "HandleReduce start" << endl;

			// Get the reducer first
			auto reducer = get_reducer_from_task_factory(request_.userid());
			reducer->impl_->output_dir = request_.output();

			if(request_.shards().size() == 0)
				return;
			
			for(auto shard : request_.shards()) {
				string path = shard.path();
				reducer->impl_->output_file = path;

				map<string, vector<string>> key_and_counts;

				if(debug_level > 1)
					cout << server_address << " " << "HandleReduce shard " << path << endl;

				// Read the file line by line
				ifstream file(path, ios::in);

				if(!file.is_open())
					cout << server_address << " " << "HandleReduce cant open file" << endl;
				else {
					string tmp;
					while(getline(file, tmp)) {
						// Split tmp into key and value separated by space
						string key, value;
						int pos = tmp.find(" ");
						key = tmp.substr(0, pos);
						value = tmp.substr(pos + 1);
						if(key_and_counts.find(key) == key_and_counts.end()) {
							key_and_counts[key] = vector<string>();
						}
						key_and_counts[key].push_back(value);
					}
				}

				file.close();

				// For all the keys, call the reducer
				for(auto key_and_count : key_and_counts) {
					string key = key_and_count.first;
					vector<string> counts = key_and_count.second;
					reducer->reduce(key, counts);
				}
			}

		}

		void Proceed() {
			if (status_ == CREATE) {
				// Make this instance progress to the PROCESS state.
				status_ = PROCESS;

				// As part of the initial CREATE state, we *request* that the system
				// start processing SayHello requests. In this request, "this" acts are
				// the tag uniquely identifying the request (so that different CallData
				// instances can serve different requests concurrently), in this case
				// the memory address of this CallData instance.
				service_ -> RequestassignTask( & ctx_, & request_, & responder_, cq_, cq_, this);
			} else if (status_ == PROCESS) {
				if(debug_level > 1)
					cout << server_address << " " << "Proceed start" << endl;

				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				new CallData(service_, cq_, server_address);

				if(debug_level > 1){
					cout << server_address << " " << "Proceed after calldata" << endl;
					cout << server_address << " " << "Proceed request received with type " << request_.type() << endl;
				}

				if(request_.type() == "MAP")
					handleMap();
				else if(request_.type() == "REDUCE")
					handleReduce();

				// And we are done! Let the gRPC runtime know we've finished, using the
				// memory address of this instance as the uniquely identifying tag for
				// the event.
				status_ = FINISH;
				responder_.Finish(response_, Status::OK, this);

				if(debug_level > 1)
					cout << server_address << " " << "Proceed end" << endl;
			} else {
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
			}
		}

	private:
		// The means of communication with the gRPC runtime for an asynchronous
		// server.
		WorkerService::AsyncService * service_;
		// The producer-consumer queue where for asynchronous server notifications.
		ServerCompletionQueue * cq_;
		// Context for the rpc, allowing to tweak aspects of it such as the use
		// of compression, authentication, as well as to send metadata back to the
		// client.
		ServerContext ctx_;

		// What we get from the client.
		WorkerQuery request_;
		// What we send back to the client.
		WorkerResponse response_;

		// The means to get back to the client.
		ServerAsyncResponseWriter < WorkerResponse > responder_;

		// Let's implement a tiny state machine with the following states.
		enum CallStatus {
			CREATE,
			PROCESS,
			FINISH
		};
		CallStatus status_; // The current serving state.

		string server_address;
	};

	// This can be run in multiple threads if needed.
	void HandleRpcs() {
		// Spawn a new CallData instance to serve new clients.
		new CallData( & service_, cq_.get(), server_address);
		void * tag; // uniquely identifies a request.
		bool ok;
		while (true) {
			if(debug_level > 1)
				cout << "HandleRpcs start" << endl;
			// Block waiting to read the next event from the completion queue. The
			// event is uniquely identified by its tag, which in this case is the
			// memory address of a CallData instance.
			// The return value of Next should always be checked. This return value
			// tells us whether there is any kind of event or cq_ is shutting down.
			GPR_ASSERT(cq_ -> Next( & tag, & ok));

			if(debug_level > 1)
				cout << "HandleRpcs received" << endl;

			GPR_ASSERT(ok);
			if(debug_level > 1)
				cout << "HandleRpcs ok" << endl;

			static_cast < CallData * > (tag) -> Proceed();

			if(debug_level > 1)
				cout << "HandleRpcs done" << endl;
		}
	}
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(string ip_addr_port) {
	server_address = ip_addr_port;

	ServerBuilder builder;
		
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		
	// Register "service_" as the instance through which we'll communicate with
	// clients. In this case it corresponds to an *asynchronous* service.
	builder.RegisterService( & service_);

	// Get hold of the completion queue used for the asynchronous communication
	// with the gRPC runtime.	
	cq_ = builder.AddCompletionQueue();
		
	// Finally assemble the server.
	server_ = builder.BuildAndStart();

	cout << "Worker server listening on " << server_address << endl;

	// Proceed to the server's main loop.
	// HandleRpcs();
}

Worker::~Worker(){
	server_ -> Shutdown();
	// Always shutdown the completion queue after the server.
	cq_ -> Shutdown();

	cout << "Shutting down worker" << endl;
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	HandleRpcs();
	return true;
}
