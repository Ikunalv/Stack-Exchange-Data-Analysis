CREATE TABLE TopUsers
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',' AS
SELECT owneruserid, SUM(score) AS TotalScore
FROM posts
GROUP BY owneruserid
ORDER BY TotalScore DESC LIMIT 10;


CREATE TABLE TopUserPosts AS
SELECT owneruserid, body, title, tags
FROM posts
WHERE owneruserid in (SELECT owneruserid FROM TopUsers)
GROUP BY owneruserid, body, title, tags;


INSERT OVERWRITE DIRECTORY '/data/hiveResults'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT owneruserid, body, title
FROM TopUserPosts
GROUP BY owneruserid, body, title;
