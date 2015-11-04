import pandas as pd
import csv
import time
import math
import getter, writer, rocchio

zero_idf = 5.0 + math.log(2365436)


label_index = getter.get_label_index()
doc_tfidf = getter.get_doc_tfidf()

centroids = rocchio.get_centroids(label_index, doc_tfidf)
writer.write_centroids(centroids)



# word_count = {}
# for each_doc in documents:
# 	for term in each_doc.term_freq.keys():
# 		if term in word_count:
# 			word_count[term] += 1
# 		else:
# 			word_count[term] = 1

# f = open("word_count", "w")
# for term, count in word_count.iteritems():
# 	f.write(term + " " + str(count) + "\n")
# f.close()

###### IDF CALCULATION #########
# fo = open("idf", "w")
# BETA = 5.0
# N = len(documents)
# content = []
# with open("doc_frequency") as f:
#     for line in f:
# 		term = line.split(" ")[0]
# 		count = float(line.split(" ")[1])
# 		idf = BETA + math.log(N/(count+1))
# 		fo.write(term + " " + str(idf)+"\n")
# fo.close()





# for each_doc in documents:
# 	for each_label in each_doc.labels:
# 		if int(each_label) in labels:
# 			labels[int(each_label)].append(each_doc.id)
# 		else:
# 			labels[int(each_label)] = [each_doc.id]

# f = open("labels", "w")
# for lbl_key, docs in labels.iteritems():
# 	print docs
# 	f.write(str(lbl_key) + " ")
# 	x = " ".join(docs)
# 	f.write(x + "\n")
# f.close()