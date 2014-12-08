REGISTER /home/nan/pig/trunk/contrib/piggybank/java/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

SET default_parallel 1;

Posts = LOAD '$INPUT' USING CSVLoader AS (Id,PostTypeId,ParentId,AcceptedAnswerId,CreationDate:chararray);

Questions = FILTER Posts BY (ISOToUnix(CreationDate) > ISOToUnix('2009-01-01T00:00:00'))
			AND (ISOToUnix(CreationDate) < ISOToUnix('2014-01-01T00:00:00'))
			AND (PostTypeId == '1');
			
Answers = FILTER Posts BY (ISOToUnix(CreationDate) > ISOToUnix('2009-01-01T00:00:00'))
		  AND (PostTypeId == '2');

AnswersGrouped = GROUP Answers BY ParentId;

FirstAnswers = FOREACH AnswersGrouped GENERATE group AS pId,
			   MIN($1.CreationDate) AS aCreationDate;
			   	   			   
QAJoined = JOIN Questions BY Id LEFT OUTER, FirstAnswers BY pId;

TimeGapPosts = FOREACH QAJoined
			   GENERATE Id,
			   CreationDate,
			   (ISOToUnix(aCreationDate)/1000 - ISOToUnix(CreationDate)/1000) AS timeGap;
		  
STORE TimeGapPosts INTO '$OUTPUT' USING PigStorage(',');