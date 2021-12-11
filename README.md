## Adwait Bauskar, Sankalp Sangle

Our flow is as follows:

1. The Master first removes the intermediate and output directories if they exist and creates them again.
2. The Master performs file sharding
3. We start the map phase:
   1. We distribute shards to all mappers in round robin manner
   2. We do an asynchronous gRPC call to all mappers containing information about the shards
   3. A mapper of the id i will create R intermediate files of the form "map_i_k.txt" where k is from 0 to R-1.
   4. A key, value pair is hashed on key to yield a number a between 0 and R-1. This key value pair is put into filename map_i_a.txt. This ensures all occurences of a word go into some file of the form map_*_a.txt
   5. Thus, in total we create (number of workers * number of expected output files) intermediate files
   6. If a worker fails (or it is slow), we detect this using gRPC deadlines (timeout). Timeout is empirically taken to be 10 seconds.
   7. All intermediate files associated with this worker are then ignored by future reducers and the map task of this worker is assigned to another active worker.
   8. We wait until all map tasks are done
4. We start the reduce phase:
    1. Every reducer is given a list of workers who have failed.
    2. Every reducer also has a reducer ID id which signifies that it has to read all files of the form "map_*_id.txt" where * is the id of all workers who have NOT failed.
    3. Now, if a reducer fails, we just send its reduction task to another non-failed receiver.
    4. In total, R output files are created in accordance to the number given in config.ini
    5. Each reducer is responsible for creating ONE output file. (NOTE: if a reducer has to take up the reduce task of another reducer which failed, then this statement will not hold)
5. The master waits for the reduce phase to finish and returns true.
6. Final output files can be found in the output directory mentioned in config.ini