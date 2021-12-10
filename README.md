Our flow is as follows:

1. The Master first removes the intermediate and output directories if they exist and creates them again.
2. The Master performs file sharding
3. The Master constructs and assigns the mapper tasks to workers and waits for them to finish
4. The Master then constructs and assigns the reducer tasks and waits for them to finish
5. Final output files can be found in the output directory mentioned in config.ini