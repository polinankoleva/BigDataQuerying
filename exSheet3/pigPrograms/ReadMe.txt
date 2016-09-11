	EX2 - ex2.pig
	
	This program takes three parameters:
	1) rdfStorageFolder - folder where rdfStorage.jar is placed
	2) input - folder from whete social graph is loaded
	3) output - folder where the result is written
	
	EXECUTION:
	pig -x local|mapreduce -param input=<input folder> 
	-param output=<output folder>
	-param rdfStorageFolder=<rdfStorage.jar folder>  ex2.pig
	
	EX3 - ex3.pig
	
	This program takes four parameters:
	1) k - the popularity
	2) rdfStorageFolder - folder where rdfStorage.jar is placed
	3) input - folder from whete social graph is loaded
	4) output - folder where the result is written
	
	EXECUTION:
	pig -x local|mapreduce -param k=<popularity> -param input=<input folder> 
	-param output=<output folder>
	-param rdfStorageFolder=<rdfStorage.jar folder>  ex3.pig 
	
	EX4 - ex4.pig
	
	This program takes three parameters:
	1) rdfStorageFolder - folder where rdfStorage.jar is placed
	2) input - folder from whete social graph is loaded
	3) output - folder where the result is written
	
	EXECUTION:
	pig -x local|mapreduce -param input=<input folder> 
	-param output=<output folder>
	-param rdfStorageFolder=<rdfStorage.jar folder>  ex4.pig 
	
Comparison b/w Pig and MapReduce:
When you are familiar with the semantics and syntax of Pig, it takes less time to write the same
tasks compared to java Map/Reduce program. Pig programs have less redundant code implementation.
In java programs we have mapper, reducer and driver which as a structure are the same for
almost every map/reduce program and we have to copy/paste them (especially a driver) every time.