import synapseclient
syn = synapseclient.login()
import argparse

def writeWholeTable(table):
	rowset = table.asRowSet()
	wikiTable =  "|".join([head['name'] for head in rowset['headers']]) + "\n"
	wikiTable =  wikiTable + "|".join(["---"]*len(rowset['headers'])) + "\n"
	for row in rowset['rows']:
		row['values'] = map(str, row['values'])
		wikiTable  = wikiTable + "|".join(row['values']) + "\n"
	return(wikiTable)


def writeProjectTables(table, getProjectIndex):
	"""
	table: 			  table query object
	getProjectIndex:  index of rowSet where you want to display the project name and synapse id link
	"""
	rowset = table.asRowSet()
	wikiTable =  "|".join([head['name'] for head in rowset['headers']]) + "\n"
	wikiTable =  wikiTable + "|".join(["---"]*len(rowset['headers'])) + "\n"
	for row in rowset['rows']:
		row['values'] = map(str, row['values'])
		try:
			projName =  syn.get(row['values'][getProjectIndex]).name
			row['values'][getProjectIndex] = "[%s](%s)" % (projName, row['values'][getProjectIndex])
		except Exception as e:
			print(e)
		wikiTable  = wikiTable + "|".join(row['values']) + "\n"
	return(wikiTable)

##Projects   syn4939478/wiki/235831

# table = syn.tableQuery('SELECT Name, Project_Title, Tumor_Focus, Synapse_ID FROM syn5867440 Where Active = True')
# firstTable = writeProjectTables(table, -1)

# table = syn.tableQuery('SELECT * FROM syn7239496')
# secondTable = writeProjectTables(table, 1)

# markdown = ("Many NTAP-funded initiatives already have data that are being uploaded to Synapse so they can be downloaded.  "
#  "Below is a table of projects with data.\n\n%s\n"
#  "In addition to the NTAP-funded grants, NTAP is working closely with [Sage Bionetworks](http://www.sagebase.org) "
#  "to carry out systems-level analysis on NF-related datasets.  "
#  "Those projects can be browsed below.\n\n%s\n"
#  "To view a complete list of NTAP projects, go to [NTAP Projects](https://www.synapse.org/#!Synapse:syn5867440/)" ) % (firstTable, secondTable)

# wikipage = syn.getWiki(syn.get('syn4990358'),"411306")
# wikipage.markdown = markdown
# syn.store(wikipage)

### Data upload   syn4939478/wiki/411657

def main(projectUploadTable, wikiPage, wikiPageId):
	# PROJECT_UPLOAD_TABLE = "syn7804884" #syn9727791
	# WIKIPAGE = "syn4939478" #syn6135075
	# WIKIPAGE_ID = "411657" #424914
	table = syn.tableQuery('select projectEntity, numberOfFiles, numberOfContributors, lateModified from %s where Active = True order by "lateModified" DESC' % projectUploadTable)
	firstTable = writeProjectTables(table, 0)
	#table = syn.tableQuery('select * from syn7805078')
	#secondTable = writeWholeTable(table)
	markdown = "Here is a summary of the latest activity by project:\n%s\n\n" % firstTable
 #"There are numerous types of data available:\n\n%s") % (firstTable, secondTable)
	wikipage = syn.getWiki(syn.get(wikiPage),wikiPageId)
	wikipage.markdown = markdown
	syn.store(wikipage)

def perform_main(args):
    main(args.projectUploadTable, args.wikiPage, args.wikiPageId)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Validate GENIE files')

    parser.add_argument("projectUploadTable", type=str, metavar="syn7804884",
                        help='Synapse Table containing project upload data')
    parser.add_argument("wikiPage", type=str, metavar="syn4939478",
                        help='Synapse Id of wikipage')
    parser.add_argument("wikiPageId", type=str,metavar="411657",
                        help='Wikipage sub Id')
    args = parser.parse_args()
    perform_main(args)