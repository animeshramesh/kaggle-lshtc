import sqlite3
from collections import defaultdict


class Label:

	def __init__(self, id, documents):
		self.id = id
		self.documents = documents


def write_labels_to_db(conn):
	labels = defaultdict(list)          # label-id -> doc1, doc2, ... , docn
	c = conn.cursor()

	print "Computing label index now."
	
	for doc in c.execute("SELECT * FROM docs"):
		doc_id = doc[0]
		labels_of_doc = doc[1].split()
		for each_label in labels_of_doc:
			labels[each_label].append(doc_id)

	print "Labels created. Writing to db now."

	i = 0
	print "Total labels = " + str(len(labels.keys()))
	for label, docs in labels.iteritems():
		docs_text = ""
		for each_doc in docs:
			docs_text += each_doc
		docs_text = docs_text[:-1]
		c.execute("INSERT INTO labels VALUES ('%s','%s', '%s')" % (label, docs_text, ""))

		# Debug statement
		if i % 25000 == 0:
			print str(i) + " labels written to db"

		i += 1
	conn.commit()
	print "Labels successfully written to db"



