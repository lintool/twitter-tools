#This file is to take run file (as an input argument) and ground truth non-redundant tweets 
#to compute the unweighted precision, recall and weighted precision per topic.
import json
from sets import Set
import argparse

parser = argparse.ArgumentParser(description='Tweet Timeline Generation (TTG) evaluation script (version 1.0)')
parser.add_argument('-q', required=True, metavar='qrels', help='qrels file')
parser.add_argument('-c', required=True, metavar='clusters', help='cluster anotations')
parser.add_argument('-r', required=True, metavar='run', help='run file')

args = parser.parse_args()
file_qrels_path = vars(args)['q']
clusters_path = vars(args)['c']
run_path = vars(args)['r']

#Take qrels to generate dictionary of {topic number:{tweetid:weight}} 
#where weight is 0(non-relevant), 1(relevant), 2(highly relevant)
qrels_dt = {}
file_qrels = open(file_qrels_path, "r")
lines = file_qrels.readlines()
for line in lines:
    line = line.strip().split()
    topic_ind = line[0]
    if topic_ind not in qrels_dt:
        qrels_dt[topic_ind] = {}
    qrels_dt[topic_ind][line[2]] = line[3]

#Take run file and generate dictionary of {topic number:Set of tweetids for that topic}
runlength = len(run_path) - run_path.index("/") - 1
clusters_run_dt = {}
file_run = open(run_path, "r")
lines = file_run.readlines()
for line in lines:
    line = line.strip().split()
    topic_ind = line[0][line[0].index("MB") + 2:]
    if topic_ind not in clusters_run_dt:
        clusters_run_dt[topic_ind] = Set()
    clusters_run_dt[topic_ind].add(line[2])

#Take ground truth, generate dictionary of {topic number:2D array of clusters of tweetids}, for each topic,
#compare tweet from each cluster with that from run file and compute unweighted precision, recall and weighted recall.
clusters_dt = {}
precision_total = 0
unweighted_recall_total = 0 
weighted_recall_total = 0
file_clusters = open(clusters_path, "r")
data = json.load(file_clusters)
topics = data["topics"]
print "runtag".ljust(runlength) + "\ttopic\tunweighted_recall weighted_recall precision"
for topic in sorted(topics.keys()):
	total_weight = 0
	credits = 0
	hit_num = 0
	topic_ind = topic[line[0].index("MB") + 2:]
	topic_ind = topic_ind.encode("utf-8")
	clusters_json = topics[topic]["clusters"]
	for i in range(len(clusters_json)):
		clusters_json[i] = [s.encode("utf-8") for s in clusters_json[i]]
	clusters_dt[topic_ind] = clusters_json
	for cluster in clusters_dt[topic_ind]:
		weight = 0
		hit_flag = 0
		for tweet in cluster:
			weight = weight + int(qrels_dt[topic_ind][tweet])
			if tweet in clusters_run_dt[topic_ind]:
				hit_flag = 1
		total_weight = total_weight + weight
		if hit_flag == 1:
			credits = credits + weight
			hit_num = hit_num + 1
			hit_flag = 0
	precision = float(hit_num) / len(clusters_run_dt[topic_ind])
	unweighted_recall = float(hit_num) / len(clusters_dt[topic_ind])
	weighted_recall = float(credits) / total_weight
	precision_total = precision_total + precision
	unweighted_recall_total = unweighted_recall_total + unweighted_recall
	weighted_recall_total = weighted_recall_total + weighted_recall
	print run_path[run_path.rindex("/") + 1:].ljust(max(runlength, 6)) + "\t" + "MB" + str(topic_ind) + "\t" + "%12.4f" % unweighted_recall + "\t" + "%12.4f" % weighted_recall + "\t" + "%10.4f" % precision
precision_mean = precision_total / len(clusters_dt)
unweighted_recall_mean = unweighted_recall_total / len(clusters_dt)
weighted_recall_mean = weighted_recall_total / len(clusters_dt)
print run_path[run_path.rindex("/") + 1:].ljust(max(runlength, 6)) + "\t" + "all".ljust(5) + "\t" + "%12.4f" % unweighted_recall_mean + "\t" + "%12.4f" % weighted_recall_mean + "\t" + "%10.4f" % precision_mean
file_run.close()
file_clusters.close()
