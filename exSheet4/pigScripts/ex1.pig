REGISTER $rdfStorageFolder/RDFStorage.jar;

-- loads social graph
indata = LOAD '$input' USING RDFStorage() AS (s, p, o) ;

-- filters the data by predicate 'foaf:birthday'
filterData = FILTER indata BY p == 'foaf:birthday' ;

-- groups birthdays by month
groupByMonth = GROUP filterData BY REGEX_EXTRACT(o, '(.*)-(.*)-(.*)', 2) ;

-- for each month counts the number of birthdays
birthdaysCountPerMonth = FOREACH groupByMonth GENERATE group, COUNT($1) AS birthdaysPerMonth ;

-- orders months by number of birthdays per month
orderedBirthdaysPerMonth = ORDER birthdaysCountPerMonth BY birthdaysPerMonth DESC ;

-- gets the month with most birthdays
theMonthWithMostBirthdays = LIMIT orderedBirthdaysPerMonth 1 ;

-- stores the result into output file
STORE theMonthWithMostBirthdays INTO '$output' ;
