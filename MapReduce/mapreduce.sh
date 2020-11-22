
hadoop jar hadoop-streaming-2.7.3.jar -files mapper1.py,reducer1.py -mapper 'python mapper1.py' -reducer 'python reducer1.py' -input /data/userData/$1 -output /data/output1


hadoop jar hadoop-streaming-2.7.3.jar -files mapper2.py,reducer2.py -mapper 'python mapper2.py' -reducer 'python reducer2.py' -input /data/output1/ -output /data/output2

hadoop jar hadoop-streaming-2.7.3.jar -files mapper3.py,reducer3.py -mapper 'python mapper3.py' -reducer 'python reducer3.py' -input /data/output2/ -output /data/output3


hadoop jar hadoop-streaming-2.7.3.jar -files mapper4.py -numReduceTasks 0 -input /data/output3/ -output /data/output4 -mapper 'python mapper4.py'

hdfs dfs -getmerge /data/output4 tfidResults/$1 

hadoop fs -rm -r /data/output*
