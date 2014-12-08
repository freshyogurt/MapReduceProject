REGISTER /home/nan/pig/trunk/contrib/piggybank/java/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

SET default_parallel 1;

Posts = LOAD '$INPUT' USING CSVLoader AS (Id,PostTypeId,ParentId,AcceptedAnswerId,CreationDate);

FilteredPosts = FILTER Posts BY (CreationDate >= ToDate('2009-01-01')) AND (CreationDate < ToDate('2014-01-01'));

PostsCopy = FOREACH FilteredPosts GENERATE Id AS aId,PostTypeId,ParentId,CreationDate AS aCreationDate;

JoinedPosts = JOIN FilteredPosts BY AcceptedAnswerId LEFT OUTER, PostsCopy BY aId;

CastPosts = FOREACH JoinedPosts
			GENERATE Id,aCreationDate AS aCreationDateString:chararray,
			CreationDate AS CreationDateString:chararray,
			GetHour(CreationDate) AS CreationHour;

TimeGapPosts = FOREACH CastPosts 
			   GENERATE Id,
			   CreationDateString,
			   (ISOToUnix(aCreationDateString)/1000 - ISOToUnix(CreationDateString)/1000) AS timeGap;

STORE TimeGapPosts INTO '$OUTPUT' USING PigStorage();