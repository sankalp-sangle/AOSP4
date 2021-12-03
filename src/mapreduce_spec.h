#pragma once

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>

using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	vector<string> worker_IPs;
	vector<string> input_files;
	string output_dir;
	int n_output_files;
	int map_kilobytes;
	string user_id;
};

// Implement parse function to split string by delimiter
inline vector<string> parse(string str, char delimiter) {
	vector<string> result;
	stringstream ss(str);
	string item;
	while (getline(ss, item, delimiter)) {
		result.push_back(item);
	}
	return result;
}

// A function to print struct MapReduceSpec
inline void helperPrint(MapReduceSpec spec) {
	cout << "Number of workers: " << spec.n_workers << endl;
	cout << "Worker IPs: ";
	for (auto ip : spec.worker_IPs) {
		cout << ip << " ";
	}
	cout << endl;
	cout << "Input files: ";
	for (auto file : spec.input_files) {
		cout << file << " ";
	}
	cout << endl;
	cout << "Output directory: " << spec.output_dir << endl;
	cout << "Number of output files: " << spec.n_output_files << endl;
	cout << "Map KiloBytes: " << spec.map_kilobytes << endl;
	cout << "User ID: " << spec.user_id << endl;
}


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	// Read line by line from config file
	ifstream config_file(config_filename);
	if (!config_file.is_open()) {
		return false;
	}
	string line;
	while (getline(config_file, line)) {
		// Split line by '='
		size_t pos = line.find('=');
		if (pos == string::npos) {
			continue;
		}
		string key = line.substr(0, pos);
		string value = line.substr(pos + 1);
		// Parse key and value
		if (key == "n_workers") {
			mr_spec.n_workers = stoi(value);
		} else if (key == "worker_ipaddr_ports") {
			mr_spec.worker_IPs = parse(value, ',');
		} else if (key == "input_files") {
			mr_spec.input_files = parse(value, ',');
		} else if (key == "output_dir") {
			mr_spec.output_dir = value;
		} else if (key == "n_output_files") {
			mr_spec.n_output_files = stoi(value);
		} else if (key == "map_kilobytes") {
			mr_spec.map_kilobytes = stoi(value);
		} else if (key == "user_id") {
			mr_spec.user_id = value;
		}
	}
	config_file.close();
	helperPrint(mr_spec);
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	if(mr_spec.n_workers <= 0) {
		return false;
	}
	if(mr_spec.worker_IPs.size() != mr_spec.n_workers) {
		return false;
	}
	if(mr_spec.input_files.size() == 0) {
		return false;
	}
	if(mr_spec.output_dir.empty()) {
		return false;
	}
	if(mr_spec.n_output_files <= 0) {
		return false;
	}
	if(mr_spec.map_kilobytes <= 0) {
		return false;
	}
	if(mr_spec.user_id.empty()) {
		return false;
	}
	return true;
}
