REGISTER /home/nan/pig/trunk/contrib/piggybank/java/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

SET default_parallel 1;

Posts = LOAD '$INPUT' USING CSVLoader AS (Id,PostTypeId,ParentId,AcceptedAnswerId,CreationDate:chararray);

FilteredPosts = FILTER Posts BY (ISOToUnix(CreationDate) > ISOToUnix('2009-01-01T00:00:00'))
				AND (ISOToUnix(CreationDate) < ISOToUnix('2014-01-01T00:00:00'));

PostsCopy = FOREACH FilteredPosts GENERATE Id AS aId,PostTypeId,ParentId AS pId,CreationDate AS aCreationDate;

JoinedPosts = JOIN FilteredPosts BY Id, PostsCopy BY pId;

TimeGapPosts = FOREACH JoinedPosts
			   GENERATE Id,
			   CreationDate,
			   (ISOToUnix(aCreationDate)/1000 - ISOToUnix(CreationDate)/1000) AS timeGap;

PostsGrouped = Group TimeGapPosts BY (Id, CreationDate);

PostsG2 = FOREACH PostsGrouped GENERATE FLATTEN(group),
		  MIN($1.timeGap) AS mintimeGap;
		  
STORE PostsG2 INTO '$OUTPUT' USING PigStorage(',');