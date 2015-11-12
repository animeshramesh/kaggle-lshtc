import sqlite3
from collections import defaultdict

class Label:

	def __init__(self, id, documents):
		self.id = id
		self.documents = documents


def get_labels(doc_details):
	labels = {}
	for doc in doc_details:
		for each_label in doc.labels:
			if each_label in labels.keys():
				labels[each_label].append(doc.id)
			else:
				labels[each_label] = [doc.id]

	return labels

def write_labels_to_db():
	labels = defaultdict(list)
	conn = sqlite3.connect('docs.db')
	c = conn.cursor()
	
	for doc in c.execute("SELECT * FROM docs"):
		doc_id = doc[0]

		# Debug statement
		if int(doc_id) % 25000 == 0:
			print doc_id 

		labels_of_doc = doc[1].split()
		for each_label in labels_of_doc:
			labels[each_label].append(doc_id)


	print "Labels created. Writing to db now."

	c.execute('''CREATE TABLE if not exists labels
             (id text, docs text)''')

	for label, docs in labels.iteritems():
		docs_text = ""
		for each_doc in docs:
			docs_text += each_doc
		docs_text = docs_text[:-1]
		c.execute("INSERT INTO labels VALUES ('%s','%s')" % (label, docs_text))
	conn.commit()
	conn.close() 

if __name__ == "__main__":
	write_labels_to_db()

