import sys
import document, centroid
import database


# Give path to CSV file as argument
if __name__ == "__main__":
	path_to_csv = sys.argv[1]

	# Fetch details of all documents and write to db
	document.write_docs_to_db(path_to_csv)

	# Compute centroid for each category
	centroid.centroid_to_db()

	# Compute denominator for each category
	document.labelDenominator()
