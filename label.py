import sqlite3
from collections import defaultdict


class Label:

	def __init__(self, id, documents):
		self.id = id
		self.documents = documents

def write_label_centroids(conn):
	print "Starting to write centroids to db label by label"
	c = conn.cursor()
	i = 0
	for result in c.execute("SELECT * FROM labels"):
		label_id = result[0]
		centroid_of_label = defaultdict(lambda: 0.0)
		doc_ids = result[1].split()
		for doc_id in doc_ids:
			size = len(doc_ids)
			c.execute("SELECT tfidf from docs where id = '%s'" % doc_id)
			tfidf_info = c.fetchone()[0].split()
			for item in tfidf_info:
				term_id = item.split(":")[0]
				tfidf_val = float(item.split(":")[1])
				centroid_of_label[term_id] += tfidf_val/size
		update_label_centroid_in_db(conn, centroid_of_label, label_id)
		i += 1
		# Debug statement
		if i % 25000 == 0:
			print str(i) + " centroids written"

	conn.commit()
	print "All centroids written to db"


def update_label_centroid_in_db(conn, centroid_of_label, label_id):
	c = conn.cursor()
	centroid_vector_text = ""
	for term_id, tfidf_val in centroid_of_label.iteritems():
		centroid_vector_text += term_id + ":" + str(tfidf_val)
	c.execute("UPDATE labels SET centroid = '%s' WHERE id = '%s'" % (centroid_vector_text,label_id))








