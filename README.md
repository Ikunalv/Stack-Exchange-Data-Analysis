# Stack Overflow data analysis

Data analysis of top 200,000 Stack Overflow queries with respect to their view count.

## Data Aquisition
The data is acquired from a platform called Stack Exchange. The data consists of the posts from Stack Overflow(https://data.stackexchange.com/stackoverflow/query/new). The following queries are executed on the platform to fetch the top 200,000 posts based on their ViewCount. The platform restricts to have 50,000 records in a query, so four queries were executed, and the output was saved into 4 CSV files, namely posts_1.csv, posts_2.csv, posts_3.csv, and posts_4.csv. The queries used for getting the data are as follows:
ou need to install the software and how to install them

```
SELECT TOP 50000 * FROM posts where ViewCount >80000 ORDER BY ViewCount DESC;
```
```
SELECT TOP 50000 * FROM posts where ViewCount <= 112523 ORDER BY ViewCount DESC;
```
```
SELECT TOP 50000 * FROM posts where ViewCount <= 66244 ORDER BY ViewCount DESC;
```
```
SELECT TOP 50000 * FROM posts where ViewCount <= 47290 ORDER BY ViewCount DESC;
```

## Extract, Transform and Load (ETL) Processes

Google Cloud Platform (GCP) was used to perform the operations. Hadoop cluster is created using DataProc, which is a managed Hadoop cluster solution by Google Cloud.

The 4 CSV files were uploaded to a temporary GCP bucket and then were loaded into the Name Node machine(cluster-f078-m) of the cluster. The files were copied to the HDFS into the <b>'/data'</b> directory.

### Pig
Pig was used to load and transform the data. 
<b>loadData.pig</b> script can be used to perform the loading and transformation of the data on the go if you don't want to do it command-by-command. Here are the commands used:
1.	<b>CSVExcelStorage</b> class is used to load the data into a variable from the HDFS. The data contains 22 columns. Arguments have been specified which enable to  process multi-line data and skip the header row.
    ~~~
    posts = load 'hdfs://cluster-f078-m/data/' using org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') as(id:int,posttypeid:int,acceptedanswerid:int, parentid:int,creationdate:DATETIME,deletiondate:DATETIME,score:int,viewcount:int,body:chararray,owneruserid:int,ownerdisplayname:chararray,lasteditoruserid:int,lasteditordisplayname:chararray,lasteditdate:DATETIME,lastactivitydate:DATETIME,title:chararray,tags:chararray,answercount:int,commentcount:int,favoritecount:int,closeddate:DATETIME,communityowneddate:DATETIME,contentlicense:chararray);
    ~~~

2.	The body column contains multi-line data which makes it difficult to load into a hive table. Removing the <b>'\n', '\t' </b>and <b>'\r'</b> characters from the body column.

    ```
    posts = foreach posts generate id,posttypeid,acceptedanswerid,parentid,creationdate,deletiondate,score,viewcount,REPLACE(body,'\\n','') as body,owneruserid,ownerdisplayname,lasteditoruserid,lasteditordisplayname,lasteditdate,lastactivitydate,title,tags,answercount,commentcount,favoritecount,closeddate,communityowneddate,contentlicense;
    ```
    ```
    posts = foreach posts generate id,posttypeid,acceptedanswerid,parentid,creationdate,deletiondate,score,viewcount,REPLACE(body,'\\t','') as body,owneruserid,ownerdisplayname,lasteditoruserid,lasteditordisplayname,lasteditdate,lastactivitydate,title,tags,answercount,commentcount,favoritecount,closeddate,communityowneddate,contentlicense;
    ```
    ```
    posts = foreach posts generate id,posttypeid,acceptedanswerid,parentid,creationdate,deletiondate,score,viewcount,REPLACE(body,'\\r','') as body,owneruserid,ownerdisplayname,lasteditoruserid,lasteditordisplayname,lasteditdate,lastactivitydate,title,tags,answercount,commentcount,favoritecount,closeddate,communityowneddate,contentlicense;
    ```

3.	The columns body, title and tags contains <b>','</b> which breaks the data and the delimiter used is also ','. Removing the ',' from the columns and removing the columns which are not required to perform the intended queries.

    ```
    formatted_csv = FOREACH posts GENERATE  id AS id, score AS score, REPLACE(body,',*','') AS body, owneruserid AS owneruserid, REPLACE(title,',*','') AS title, REPLACE(tags,',*','') AS tags;
    ```

4.	Removing all the records having null entries from the <b>OwnerUserID</b> and<b> Score</b> columns

    ```
    valid_csv = FILTER formatted_csv BY (owneruserid IS NOT NULL) AND (score IS NOT NULL);
    ```

5.	Loading the processed data into a hive table(Defined in the next section) using <b>HCatalog</b> class

    ```
    store valid_csv into 'userdb.posts' using org.apache.hive.hcatalog.pig.HCatStorer();
    ```
### Hive
Hive was used to store the data and perform queries on it.
Command to create a table to store the data:
```
create table posts(id int, score int, body String, owneruserid Int, title String, tags String) 
row format delimited 
FIELDS TERMINATED BY ',';
```
Some data analysis done using Hive query language
1. The <b>top 10 posts</b> by <b>score</b>
    ```
    SELECT id, score, owneruserid,title from posts 
    ORDER BY score DESC
    LIMIT 10;
    ```
2.	The <b>top 10 users</b> by post <b>score</b>
    ```
    SELECT owneruserid, SUM(score) AS Total_Score from posts 
    GROUP BY owneruserid
    ORDER BY Total_Score DESC
    LIMIT 10;
    ```
3.	The <b>number of distinct users</b>, who used the word <b>'hadoop'</b> in one of their posts
    ```
    SELECT COUNT(DISTINCT owneruserid) AS unique_user_Count from posts 
    WHERE (body like '%hadoop%' OR title like '%hadoop%' or tags like '%hadoop%');
    ```
## TF-IDF (Term Frequency-Inverted Document Frequency)
The calculation of TF-IDF per User of the top ten users involved the fetching of top ten users and storing in a separate table called TopUsers. 

```
CREATE TABLE TopUsers
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' AS
SELECT owneruserid, SUM(score) AS TotalScore
FROM posts
GROUP BY owneruserid
ORDER BY TotalScore DESC LIMIT 10;
```

The **OwnerUserId** from this TopUsers table is then used to query the top User's all of the posts' Body, Title and Tags and stored in a table called TopUserPosts.

```	
CREATE TABLE TopUserPosts AS
SELECT owneruserid, body, title, tags
FROM posts
WHERE owneruserid in (SELECT owneruserid FROM TopUsers)
GROUP BY owneruserid, body, title, tags;
```

This **TopUserPosts** is then stored onto an HDFS directory. 
```	
INSERT OVERWRITE DIRECTORY '/data/hiveResults'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT owneruserid, body, title
FROM TopUserPosts
GROUP BY owneruserid, body, title;
```

The result partitions from this HDFS directory is merged using the command: 
hdfs dfs -getmerge /data/hiveResults hiveResults.csv 
and stored at an NFS directory. Hive was used to fetch the TopUserPosts and the queries are enclosed within getTopUserPosts.sql file.

The results from the above query, hiveResults.csv consists of all the User's records in the same file which was split into their respective user files to be used as input to TF IDF program. This splitting was done in python script - splitTopUserPosts python file.
The results of the splitTopUserPosts script is fed file by file as input to TF IDF mappers using the mapreduce script. The collection of TF IDF mappers reducer programs are stored in MapReduce folder. The commands ran to compute the TF IDF is a script called mapreduce.sh 

bash mapreduce.sh {Name of the txt file}

The mapreduce.sh script used the following commands for executing mappers and reducers:
```
hadoop jar hadoop-streaming-2.7.3.jar -files mapper1.py,reducer1.py -mapper 'python mapper1.py' -reducer 'python reducer1.py' -input /data/userData/$1 -output /data/output1
```
```
hadoop jar hadoop-streaming-2.7.3.jar -files mapper2.py,reducer2.py -mapper 'python mapper2.py' -reducer 'python reducer2.py' -input /data/output1/ -output /data/output2
```
```
hadoop jar hadoop-streaming-2.7.3.jar -files mapper3.py,reducer3.py -mapper 'python mapper3.py' -reducer 'python reducer3.py' -input /data/output2/ -output /data/output3
```
```
hadoop jar hadoop-streaming-2.7.3.jar -files mapper4.py -numReduceTasks 0 -input /data/output3/ -output /data/output4 -mapper 'python mapper4.py'
```

The results from the above MapReduce gave results to ten files for each user. It contained the top terms and their weights. To fetch the top ten terms of each user, sortResults.py was run. All the final results for the TF-IDF score of the top 10 users are saved in the TFIDF_Results.txt file. 


