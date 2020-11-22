create table posts(id int, score int, body String, owneruserid Int, title String, tags String) 
row format delimited 
FIELDS TERMINATED BY ',';

SELECT id, score, owneruserid,title from posts
ORDER BY score DESC
LIMIT 10;

SELECT owneruserid, SUM(score) AS Total_Score from posts 
GROUP BY owneruserid
ORDER BY Total_Score DESC
LIMIT 10;

SELECT COUNT(DISTINCT owneruserid) AS unique_user_Count from posts 
WHERE (body like '%hadoop%' OR title like '%hadoop%' or tags like '%hadoop%');

