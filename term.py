import sqlite3
from collections import defaultdict
import math

def write_terms_to_db():
	terms = defaultdict(list)
	conn = sqlite3.connect('docs.db')
	c = conn.cursor()

	for doc in c.execute("SELECT * FROM docs"):
		doc_id = doc[0]

		# Debug statement
		if int(doc_id) % 25000 == 0:
			print doc_id 

		freq = doc[2].split()
		for each_val in freq:
			term_id = each_val.split(":")[0]
			terms[term_id] = doc_id

	print "Terms created. Writing to db now."

	c.execute('''CREATE TABLE if not exists terms
             (id text, docs text, idf real)''')

	for term, docs in terms.iteritems():
		docs_text = ""
		for each_doc in docs:
			docs_text += each_doc
		docs_text = docs_text[:-1]
		c.execute("INSERT INTO terms VALUES ('%s','%s', -1)" % (term, docs_text))
	conn.commit()
	conn.close() 


def compute_idf():
	BETA = 5.0
	idfs = defaultdict(float)
	conn = sqlite3.connect('docs.db')
	c = conn.cursor()

	# Counting the total number of documents in the db
	c.execute('''SELECT COUNT(*) FROM docs''')
	N = int(c.fetchone()[0])

	for term in c.execute("SELECT * FROM terms"):
		term_id = term[0]
		docs = term[1]
		count = len(docs)
		idf = BETA + math.log(N/(count + 1))
		idfs[term_id] = idf

	return idfs

def write_idf_to_db(idfs):
	conn = sqlite3.connect('docs.db')
	c = conn.cursor()

	# c.execute("CREATE INDEX MyIndex ON terms(id)");
	# conn.commit()

	for term, idf in idfs.iteritems():
		print "Updating idf for term " + term
		c.execute("UPDATE terms SET idf = '%f' WHERE id = '%s'" % (idf,term))

	conn.commit()
	conn.close() 


if __name__ == "__main__":
	#write_terms_to_db()
	idfs = compute_idf()
	write_idf_to_db(idfs)
