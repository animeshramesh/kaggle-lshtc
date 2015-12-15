from pymongo import MongoClient
from collections import defaultdict
import time
import csv
import math

client = MongoClient(connect=False)
db = client.kaggle
BETA = 0.5

# First time training : Writes to db : docs[id,label,terms_freq,tfidf], terms[id,docs,idf], labels[id,docs]
def write_docs_to_db(path_to_csv):
	docs_coll = db.docs
	print "Fetching docs line by line and writing to db ..."
	start_time = time.clock()
	i = 0
	with open(path_to_csv, 'rb') as csv_file:
		csv_reader = csv.reader(csv_file)
		labels = defaultdict(list)
		terms = defaultdict(list)
		for row in csv_reader:
			try:
				labels_list = []
				terms_freq = {}
				term_freq_text = ""
				for val in row:
					if ":" in val:
						first = val.split()[0]
						labels_list.append(int(first))
						labels[first.strip()].append(i)
						term_freq_text = val[val.strip().index(' ')+1:]
						for tf in term_freq_text.split():
							temp = tf.split(':')
							terms_freq[temp[0]] = int(temp[1])
							terms[temp[0].strip()].append(i)
					else:
						if ',' in val:
							labels_list.append(int(val[:-1]))
							labels[val[:-1].strip()].append(i)
						else:
							labels_list.append(int(val))
							labels[val.strip()].append(i)
	
				if i % 100000 == 0:
					print str(i) + " documents written to db"


				docs_coll.insert_one(
					{
					'_id' : i,
					'labels' : labels_list,
					'terms_freq' : terms_freq,
					'tfidf' : 0
					}
				)

			except Exception,e:
				print e
			i += 1
	end_time = time.clock()
	print "Writing documents to DB took " + str(end_time - start_time) + " seconds"
	labels_coll = db.labels
	start_time = time.clock()
	for each_label, docs in labels.iteritems():
		labels_coll.insert_one(
			{
			'_id' : int(each_label),
			'docs' : docs,
			'centroid' : 0.0
			}
		)
	end_time = time.clock()
	print "Time taken to write labels " + str(end_time - start_time) + " seconds"
	terms_coll = db.terms
	start_time = time.clock()
	for each_term, docs in terms.iteritems():
		terms_coll.insert_one(
			{
			'_id' : int(each_term),
			'docs' : docs,
			'idf' : BETA + math.log(i / (len(docs) + 1))
			}
		)
	end_time = time.clock()
	print "Time taken to write terms " + str(end_time - start_time) + " seconds"
	start_time = time.clock()
	j = 0
	for document in docs_coll.find(modifiers={"$snapshot": True}):
		tfidf_vector = defaultdict()
		for term,freq in document['terms_freq'].iteritems():
			count = len(terms[term])
			tfidf_vector[term] = math.log(freq + 1) * ( BETA + math.log(i / count + 1))
		docs_coll.update_one(
			{'_id' : document['_id']},
			{
				'$set': {
					'tfidf':tfidf_vector
				}
			}
		)
		j += 1
		if j%100000==0:
			print str(j) + " documents tfidf vector computed"
	end_time = time.clock()
	print "Time taken to compute tfidf " + str(end_time-start_time) + " seconds"

# Returns tfidf[doc_id] = "term1:tfidf1 term2:tfidf2 ..."
def compute_tfidf(conn):

	tfidf_vectors = {}
	c = conn.cursor()
	print "Starting to compute TFIDF values. [for each term in each doc]"
	i = 0
	for doc in c.execute("SELECT * FROM docs"):
		doc_id = doc[0]
		tfidf_text = ""
		term_freqs_info = doc[2].split()
		for item in term_freqs_info:
			term_id = item.split(":")[0]
			freq = int(item.split(":")[1])
			c.execute("SELECT idf from terms where id = '%s'" % (term_id))
			idf = float(c.fetchone()[0])
			tfidf = math.log(freq + 1) * idf
			tfidf_text += term_id + ":" + str(tfidf) + " "
		tfidf_text = tfidf_text[:-1]
		tfidf_vectors[doc_id] = tfidf_text
		i += 1

		# Debug statement
		if i % 25000 == 0:
			print "Tfidf vectors computed for " + str(i) + " documents"
	print "Computation of TFIDF done"
	return tfidf_vectors


def write_tfidf_vectors_to_db(tfidf_vectors, conn):
	c = conn.cursor()
	print "Writing TFIDF values to db. [docs table]"
	i = 0
	for doc_id, tfidf_vector in tfidf_vectors.iteritems():
		c.execute("UPDATE docs SET tfidf = '%s' WHERE id = '%s'" % (tfidf_vector,doc_id))
		
		i += 1
		# Debug statement
		if i % 25000 == 0:
			print "Tfidf vectors updated in DB for " + str(i) + " documents"
	conn.commit()
	print "TFIDF values successfully written to db"





