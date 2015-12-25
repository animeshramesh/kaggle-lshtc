from pymongo import MongoClient
from collections import defaultdict
import os, time

client = MongoClient()
db = client.kaggle

# Pass a test document in the form of a dictionary representing it's feature vector
def ncc(test_doc, size):
	res = defaultdict(float)
	test_denominator = 0.0
	count = 0
	for term,tf in test_doc.iteritems():
		test_denominator += tf*tf
	for label in db.labels.find().batch_size(5000):
		cosine = 0.0
		label_denominator = label['label_denominator']
		for term,tf in test_doc.iteritems():
			if term in label['centroid']:
				cosine = cosine + tf * label['centroid'][term]
		cosine = cosine/(label_denominator*test_denominator)
		res[str(label['_id'])] = cosine
		if len(res) == size+1:
			toRemove = ""
			for a in sorted(res, key=res.get):
				toRemove = a
				break
			del res[toRemove]
		count += 1
	return res
