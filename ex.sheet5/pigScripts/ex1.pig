-- registers the rdf storage jar
REGISTER $rdfStorageJar;

-- registers the user-defined functions jar
REGISTER $udfJar;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- gets all birthdays 
birthdayEdges = FILTER indata BY p == 'foaf:birthday';

-- extracts only birthdays
birthdays = FOREACH birthdayEdges GENERATE o;

-- computes average age of a user
count = FOREACH (group birthdays all) GENERATE udf.AVERAGE_AGE(birthdays) ;

-- stores the result into output file
STORE count INTO '$output' ;