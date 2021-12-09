#pragma once

#include <string>
#include <iostream>
#include <fstream>

using namespace std;

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		int n_reducers;
		string output_dir;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
	
}


/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	// std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
	size_t hashed_number = hash<string>{}(key);
	size_t modded_number = hashed_number % n_reducers;

	string intermediate_file = "intermediate/" + to_string(modded_number) + ".txt";

	// Append to intermediate file
	ofstream outfile;
	outfile.open(intermediate_file, ios_base::app);
	outfile << key << " " << val << endl;
	outfile.close();

}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		string output_file;
		string output_dir;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	// std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
	ofstream outfile;
	outfile.open(output_dir + "/" + output_file + ".txt", ios_base::app);
	outfile << key << " " << val << endl;
	outfile.close();
}
