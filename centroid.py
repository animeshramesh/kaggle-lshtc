from pymongo import MongoClient
from collections import defaultdict
import time
import csv
import math

client = MongoClient(connect=False)
db = client.kaggle
BETA = 0.5

# Second pass : Centroids
def centroid_to_db():
	counter = 0
	time1 = time.time()
	for label in db.labels.find(modifiers={"$snapshot": True}).batch_size(500):
		counter += 1
		if counter%1000 == 0:
			print "Done: ",counter," Time wasted: ",time.time()-time1
		if not label['centroid'] == 0:
			continue
		centroid_temp = defaultdict(lambda: 0.0)
		docs = label['docs']
		size = len(docs)
		for doc_id in docs:
			doc = db.docs.find_one({'_id':doc_id})
			for term,tfidf in doc['tfidf'].iteritems():
				centroid_temp[str(int(term))] = centroid_temp[str(int(term))] + tfidf/size
		db.labels.update_one(
				{'_id' : label['_id']},
				{
					'$set': {
						'centroid' : centroid_temp
					}
				}
			)
