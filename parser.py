import csv
import document
import sys
import time


# Remove the first row containing 'Data' in the CSV
def get_docs(path_to_csv):
	i = 0
	start_time = time.clock()
	with open(path_to_csv, 'rb') as csv_file:
		csv_reader = csv.reader(csv_file)
		for row in csv_reader:
			labels = []					# Creating empty list of labels for this document
			term_freq = {}
			for val in row:
				if ":" in val:
					for subval in val.split():		# term-id:term-frequency
						if ":" in subval:
							term_id = subval.split(':')[0]
							freq = int(subval.split(':')[1])
							term_freq[term_id] = freq
						else:
							labels.append(subval.strip())

				else:
					labels.append(val.strip())

			if (i%25000 == 0):
				print str(i) + " documents done"
			
			i += 1
	end_time = time.clock()
	print "Fetching documents took" + str(end_time - start_time) + "seconds" 


# Give path to CSV file as argument in terminal
if __name__ == "__main__":
	path_to_csv = sys.argv[1]
	get_docs(path_to_csv)