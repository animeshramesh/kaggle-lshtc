

class Document:
	id = -1
	labels = []
	term_freq = {}
	tfidf = {}

	def __init__(self, id, labels, term_freq):
		self.id = id
		self.labels = labels
		self.term_freq = term_freq

def get_label_index():
	labels = {}
	with open("labels") as f:
		for line in f:
			x = line.split(" ")
			label_id = x[0]
			docs = []
			for doc in x[1:]:
				docs.append(doc.rstrip())
			labels[label_id] = docs
	return labels
		


def get_doc_tfidf():
	doc_tfidf = []
	with open("tfidf") as f:
		for line in f:
			tfidf = {}
			x = line.split(" ")
			doc_id = x[0]
			for pair in x[1:]:
				z = pair.split(":")
				if len(z) == 2:
					term = z[0].rstrip()
					term_tfidf = z[1].rstrip()
					tfidf[term] = float(term_tfidf)
			doc_tfidf.append(tfidf)
	return doc_tfidf


def get_idf():
	idfs = {}
	with open("idf") as f:
		for line in f:
			term = line.split(" ")[0]
			idf = float(line.split(" ")[1])
			idfs[term] = idf
	return idfs

def parse_csv():
	documents = []
	labels = {}		# label_index : [doc1, doc2, doc3]

	i = 0
	inp = open("../train.csv", 'r')
	reader = csv.reader(inp, delimiter=' ')

	for line in reader:
		if len(line) < 2:
			continue

		labels_for_this_doc = []
		terms_of_this_doc = {}
		for cell in line:
			if ":" in cell:
				a = cell.split(':')
				terms_of_this_doc[a[0]] = float(a[1])
			else:
				x = cell.replace(",", "")
				x = x.replace(" ", "")
				labels_for_this_doc.append(x)
		documents.append(Document(str(i), labels_for_this_doc, terms_of_this_doc))
		i +=1
	return documents




