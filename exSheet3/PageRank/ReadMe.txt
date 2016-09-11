Task 1a (Retrieval friends of an user)

   - It receives three parameters in such order:
   1) input folder where the initial data can be found - social network graph
   3) output folder where the result data will be stored 
   4) number of reducers
   
   - Execution: hadoop jar PageRank-1.0.jar 1 <input dir> <output dir> <num of reducers>


Task 1b (Page Rank computation with fixed iterations)

   - It receives four parameters in such order:
   1) input folder where the initial data can be found - social network graph
   2) output folder 1 where the result data will be stored 
   2) output folder 2 where the result data will be stored 
   3) number of reducers
   Because we can have multiple consecutive jobs - each of them uses as an input, 
   the output of the previous one, we need two output folders which to alternate.
   For example, outputFolder1 - input of a job/ outputFolder2 - 
   output of a job and its consecutive job uses outputFolder2 - input/ outputFolder1 - output. 
   Finally, the result data will be in either outputFolder1 or outputFolder2.
   
   - Execution: hadoop jar PageRank-1.0.jar 2 <input dir> <output dir 1> <output dir 2> <num of reducers>
    
Task 1c (Page Rank computation with specific termination condition)

   - It receives five parameters in such order:
   1) input folder where the initial data can be found - social network graph
   2) output folder 1 where the result data will be stored 
   3) output folder 2 where the result data will be stored 
   Because we can have multiple consecutive jobs - each of them uses as an input, 
   the output of the previous one, we need two output folders which to alternate.
   For example, outputFolder1 - input of a job/ outputFolder2 - 
   output of a job and its consecutive job uses outputFolder2 - input/ outputFolder1 - output. 
   Finally, the result data will be in either outputFolder1 or outputFolder2.
   4) temporary folder which will be used only for intermediate results. It will be deleted after the execution.
   4) number of reducers
   
   - Execution: hadoop jar PageRank-1.0.jar 3 <input dir> <output dir 1> <output dir 2> <temp dir> <num of reducers>
