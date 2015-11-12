import parser
import pickle
import math
import sqlite3

class Document:
	id = -1
	labels = []			# This document belongs to these labels			
	term_freq = {}
	tfidf = {}

	def __init__(self, id, labels, term_freq):
		self.id = id
		self.term_freq = term_freq
		self.labels = labels


# Returns tfidf[doc_id] = "term1:tfidf1 term2:tfidf2 ..."
def compute_tfidf():
	tfidf_vectors = {}
	conn = sqlite3.connect('docs.db')
	c = conn.cursor()

	# c.execute("CREATE INDEX MyDocIndex ON docs(id)");
	# conn.commit()

	i = 0
	for doc in c.execute("SELECT * FROM docs"):
		doc_id = doc[0]
		tfidf_text = ""
		term_freqs = doc[2].split()
		for item in term_freqs:
			term_id = item.split(":")[0]
			freq = int(item.split(":")[1])
			c.execute("SELECT idf from terms where id = '%s'" % (term_id))
			idf = float(c.fetchone()[0])
			tfidf = math.log(freq + 1) * idf
			tfidf_text += str(tfidf) + " "
		tfidf_text = tfidf_text[:-1]
		tfidf_vectors[doc_id] = tfidf_text
		i += 1

		# Debug statement
		if i % 25000 == 0:
			print "Tfidf vectors computed for " + str(i) + " documents"

	print i 
	conn.commit()
	conn.close() 

	return tfidf_vectors

def write_tfidf_vectors_to_db(tfidf_vectors):
	conn = sqlite3.connect('docs.db')
	c = conn.cursor()

	i = 0
	for doc_id, tfidf_vector in tfidf_vectors.iteritems():
		print "Updating tfidf vector for doc " + doc_id
		c.execute("UPDATE docs SET tfidf = '%s' WHERE id = '%s'" % (tfidf_vector,doc_id))
		
		i += 1
		# Debug statement
		if i % 25000 == 0:
			print "Tfidf vectors updated in DB for " + str(i) + " documents"


	conn.commit()
	conn.close() 

if __name__ == "__main__":
	tfidf_vectors = compute_tfidf()
	write_tfidf_vectors_to_db(tfidf_vectors)




