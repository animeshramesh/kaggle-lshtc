from pymongo import MongoClient
from sortedcontainers import SortedDict
from collections import defaultdict
import time
import cPickle
import os

'''
def get_centroids(label_index, doc_tfidf):
	centroid_collection = {}
	for label_id, docs in label_index.iteritems():
		centroid = {}
		for doc_id in docs:
			size = len(docs)
			for term, tfidf in doc_tfidf[int(doc_id)].iteritems():
				if term in centroid.keys():
					centroid[term] += float(tfidf)/size
				else:
					centroid[term] = float(tfidf)/size
		centroid_collection[label_id] = centroid
	return centroid_collection
'''

client = MongoClient()
db = client.kaggle
def ncc(test_doc):
	time1 = time.time()
	res = defaultdict(float)
	test_denominator = 0.0
	count = 0
	for term,tf in test_doc.iteritems():
		test_denominator += tf*tf
	for label in db.labels.find().batch_size(6000):
		cosine = 0.0
		label_denominator = label['label_denominator']
		for term,tf in test_doc.iteritems():
			if term in label['centroid']:
				cosine = cosine + tf * label['centroid'][term]
		cosine = cosine/(label_denominator*test_denominator)
		res[str(label['_id'])] = cosine
		if len(res) == 10:
			toRemove = ""
			for a in sorted(res, key=res.get):
				toRemove = a
				break
			del res[toRemove]
		count += 1
#		if count % 10000 == 0:
#			print count
	print "TIME: ",time.time()-time1
	return res

test_doc1 = { "153" : 1, "199" : 1, "198" : 1, "210" : 1, "211" : 2, "195" : 1, "194" : 1, "197" : 1, "196" : 1, "191" : 1, "190" : 1, "193" : 1, "192" : 1, "212" : 1, "175" : 1, "213" : 1, "214" : 1, "186" : 4, "187" : 1, "185" : 1, "188" : 2, "189" : 1, "201" : 3, "200" : 1, "203" : 1, "202" : 1, "205" : 1, "204" : 1, "207" : 1, "206" : 1, "209" : 1, "208" : 1, "53" : 1 }
res =  ncc(test_doc1)
for a in sorted(res, key=res.get):
	print a," ",res[a]

