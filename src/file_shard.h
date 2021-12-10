#pragma once

#include <vector>
#include "mapreduce_spec.h"
#include "sys/stat.h"
#include <math.h>


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct FileInfo {
     string filename;
     int start_offset;
     int end_offset;
};

struct FileShard {
     vector<FileInfo> file_info;
};

// Implement print_file_shard() function
inline void print_file_shard(FileShard file_shard) {
     int total_size = 0;
     for (int i = 0; i < file_shard.file_info.size(); i++) {
          cout << "File: " << file_shard.file_info[i].filename << " Start: " << file_shard.file_info[i].start_offset << " End: " << file_shard.file_info[i].end_offset << endl;
          total_size += file_shard.file_info[i].end_offset - file_shard.file_info[i].start_offset;
     }
     cout << "Total Size: " << total_size << endl;
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
     // Get sum of total size of input files in mr_spec.input_files
     int total_size = 0;
     for (int i = 0; i < mr_spec.input_files.size(); i++) {
          string file = mr_spec.input_files[i];
          // Get size of file on disk
          struct stat st;
          stat(file.c_str(), &st);
          total_size += st.st_size;
          cout << "File: " << file << " Size: " << st.st_size << endl;
     }

     int total_shards = (int)ceil( (double)total_size / ((double) mr_spec.map_kilobytes * 1000));
     cout << "Total shards: " << total_shards << endl;
     
     // Where we are in the sharding process can be defined by file_index and start offset in the file
     int file_index = 0;
     int start_offset = 0;
     int read_size = 0;

     for(int shard_number = 0; shard_number < total_shards; shard_number++) {
          FileShard file_shard;

          while(read_size < mr_spec.map_kilobytes * 1000) {
               if(file_index >= mr_spec.input_files.size()) {
                    break;
               }
               string file = mr_spec.input_files[file_index];

               // Read file line by line starting from start_offset
               ifstream input_file(file);
               input_file.seekg(start_offset);
               string line;
               int current_offset = start_offset;

               bool file_broken_midway = false;
               while(getline(input_file, line)) {
                    read_size += (line.size() + 1);
                    if(read_size >= mr_spec.map_kilobytes * 1000) {
                         current_offset += line.size() + 1;
                         file_shard.file_info.push_back({file, start_offset, current_offset-1});
                         // Shard Limit reached but not at end of file
                         start_offset = current_offset;
                         file_broken_midway = true;
                         read_size = 0;
                         break;
                    } else {
                         current_offset += (line.size()+1);
                    }
               }
               if(!file_broken_midway) {
                    file_shard.file_info.push_back({file, start_offset, current_offset-1});
                    file_index++;
                    start_offset = 0;
               } else {
                    start_offset = current_offset;
                    break;
               }
          }
          fileShards.push_back(file_shard);
     }

     for(int i = 0; i < fileShards.size(); i++) {
          cout << "---------------------------------------------" << endl;
          cout << "Shard: " << (i+1) << endl;
          print_file_shard(fileShards[i]);
     }
	return true;
}
