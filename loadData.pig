--Loading the data from all the CSVs
posts = load 'hdfs://cluster-f078-m/data/' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') as(id:int,posttypeid:int,acceptedanswerid:int,parentid:int,creationdate:DATETIME,deletiondate:DATETIME,score:int,viewcount:int,body:chararray,owneruserid:int,ownerdisplayname:chararray,lasteditoruserid:int,lasteditordisplayname:chararray,lasteditdate:DATETIME,lastactivitydate:DATETIME,title:chararray,tags:chararray,answercount:int,commentcount:int,favoritecount:int,closeddate:DATETIME,communityowneddate:DATETIME,contentlicense:chararray);

--Removing '\n' characters from body column
posts = foreach posts generate id,posttypeid,acceptedanswerid,parentid,creationdate,deletiondate,score,viewcount,REPLACE(body,'\\n','') as body,owneruserid,ownerdisplayname,lasteditoruserid,lasteditordisplayname,lasteditdate,lastactivitydate,title,tags,answercount,commentcount,favoritecount,closeddate,communityowneddate,contentlicense;

--Removing '\t' characters from body column
posts = foreach posts generate id,posttypeid,acceptedanswerid,parentid,creationdate,deletiondate,score,viewcount,REPLACE(body,'\\t','') as body,owneruserid,ownerdisplayname,lasteditoruserid,lasteditordisplayname,lasteditdate,lastactivitydate,title,tags,answercount,commentcount,favoritecount,closeddate,communityowneddate,contentlicense;

--Removing '\t' characters from body column
posts = foreach posts generate id,posttypeid,acceptedanswerid,parentid,creationdate,deletiondate,score,viewcount,REPLACE(body,'\\r','') as body,owneruserid,ownerdisplayname,lasteditoruserid,lasteditordisplayname,lasteditdate,lastactivitydate,title,tags,answercount,commentcount,favoritecount,closeddate,communityowneddate,contentlicense;

--Removing ',' from body,title and tags columns
formatted_csv = FOREACH posts GENERATE  id AS id, score AS score, REPLACE(body,',*','') AS body, owneruserid AS owneruserid, REPLACE(title,',*','') AS title, REPLACE(tags,',*','') AS tags;

--Removing the NULL entries 
valid_csv = FILTER formattedcsv BY (owneruserid IS NOT NULL) AND (score IS NOT NULL);

--Storing the data into a HIVE table
store valid_csv into 'userdb.posts' using org.apache.hive.hcatalog.pig.HCatStorer();