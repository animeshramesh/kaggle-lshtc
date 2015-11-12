import parser
import pickle

class Document:
	id = -1
	labels = []			# This document belongs to these labels			
	term_freq = {}
	tfidf = {}

	def __init__(self, id, labels, term_freq):
		self.id = id
		self.term_freq = term_freq
		self.labels = labels


# Loading docs from file for faster access (not csv)
def load_docs():
	docs = pickle.load(open( "docs.pickle", "rb" ))
	return docs 


# Writing docs to file. (not csv)
def write_docs(docs):
	pickle.dump(docs, open( "docs.pickle", "wb" ))

