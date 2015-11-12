import csv
import document
import sys
import time
import document
import sqlite3


# Remove the first row containing 'Data' in the CSV
def get_docs(path_to_csv, conn):
	c = conn.cursor()
	c.execute('''CREATE TABLE docs
             (id text, labels text, term_freq text, tfidf text)''')
	i = 0
	start_time = time.clock()
	docs = []
	with open(path_to_csv, 'rb') as csv_file:
		csv_reader = csv.reader(csv_file)
		for row in csv_reader:
			labels = []					# Creating empty list of labels for this document
			labels_text = ""
			term_freq = {}
			term_freq_text = ""
			for val in row:
				if ":" in val:
					for subval in val.split():		# term-id:term-frequency
						if ":" in subval:
							term_id = subval.split(':')[0]
							freq = int(subval.split(':')[1])
							term_freq[term_id] = freq
							term_freq_text += subval + " "
						else:
							labels.append(subval.strip())
							labels_text += subval.strip() + " "

				else:
					labels.append(val.strip())
					labels_text += val.strip() + " "


			if (i%100000 == 0):
				print str(i) + " documents done"
			#docs.append(document.Document(str(i), labels, term_freq))
			labels_text = labels_text[:-1]
			term_freq_text = term_freq_text[:-1]
			c.execute("INSERT INTO docs VALUES ('%s','%s','%s','%s')" % (str(i), labels_text, term_freq_text, ""))
			i += 1
	end_time = time.clock()
	print "Fetching documents took " + str(end_time - start_time) + " seconds"
	#document.write_docs(docs)
	conn.commit()
	conn.close() 


# Give path to CSV file as argument in terminal
if __name__ == "__main__":
	conn = sqlite3.connect('docs.db')
	path_to_csv = sys.argv[1]
	get_docs(path_to_csv, conn)