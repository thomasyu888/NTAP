import synapseclient
from synapseclient import Table
import pandas as pd
syn = synapseclient.login()

#Table 1:
#Project Entity -- Number of Files uploaded -- Number of contributors  -- Date of latest upload
#> synapseid, number of files total to the project, number of contributors, last modified on date
#https://www.synapse.org/#!Synapse:syn4939478/wiki/411657 

FILEVIEW = "syn9692006"
#Must create a project upload table first
PROJECT_UPLOAD_TABLE = "syn9727791"
PROJECT_ID_KEY = "projectId"

allprojects = syn.tableQuery('SELECT * FROM %s' % FILEVIEW)
allprojects_df = allprojects.asDataFrame()
allprojects_df = allprojects_df.drop_duplicates(PROJECT_ID_KEY)
allprojects_df['Active'] = True

projectTracker = syn.tableQuery('select * from %s' % PROJECT_UPLOAD_TABLE)
projectTrackerDf = projectTracker.asDataFrame()
projectTrackerDf['lateModified'] = projectTrackerDf['lateModified'].fillna(0)
removeSamples = []
for index, synId in enumerate(allprojects_df[PROJECT_ID_KEY]):
	temp = syn.chunkedQuery('select id, createdByPrincipalId, modifiedOn from file where projectId == "%s"' % synId)
	modifiedOn = []
	createdBy = []
	count = 0
	for x in temp:
		modifiedOn.append(x['file.modifiedOn'])
		createdBy.append(x['file.createdByPrincipalId'])
		count = count + 1
	if len(modifiedOn) > 0:
		mod = max(modifiedOn)
	else:
		mod = 0
	oldValues = projectTrackerDf[projectTrackerDf['projectEntity'] == synId]
	newValues = [synId, count, len(set(createdBy)), mod, allprojects_df['Active'][index]]
	if not oldValues.empty:
		if not all([old == new for old, new in zip(oldValues.values[0], newValues)]):
			projectTrackerDf[projectTrackerDf['projectEntity'] == synId] = newValues
		else:
			removeSamples.append(synId)
	else:
		projectTrackerDf = projectTrackerDf.append(pd.DataFrame([newValues],columns=['projectEntity','numberOfFiles','numberOfContributors','lateModified','Active']))

newUploads = projectTrackerDf[~projectTrackerDf['projectEntity'].isin(removeSamples)]
if not newUploads.empty:
	newUploads['lateModified'] = newUploads['lateModified'].apply(int)
	newUploads['numberOfFiles'] = newUploads['numberOfFiles'].apply(int)
	newUploads['numberOfContributors'] = newUploads['numberOfContributors'].apply(int)
	newUploads['lateModified'][newUploads['lateModified'] == 0] = ""
	schema = syn.get(PROJECT_UPLOAD_TABLE)
	tablestore = Table(schema, newUploads, etag=projectTracker.etag)
	tablestore = syn.store(tablestore)
else:
	print("No updates!")





annotations = syn.tableQuery('select * from syn9692006')
annot_table = annotations.asDataFrame()

assaysNumSynId = {}
assaysNF2genotype =  {}
assaysTumorType = {}
assaysNF1genotype = {}

assays = set(annot_table['assay'])

for i in assays:
	if pd.isnull(i):
		numAssay = len(set(annot_table['id'][annot_table['assay'].isnull()]))
		numnf1 = len(set(annot_table['nf1Genotype'][annot_table['assay'].isnull()]))
		numnf2 = len(set(annot_table['nf2Genotype'][annot_table['assay'].isnull()]))
		numtumor = len(set(annot_table['tumorType'][annot_table['assay'].isnull()]))

	else:		
		numAssay = len(set(annot_table['id'][annot_table['assay'] == i]))
		numnf1 = len(set(annot_table['nf1Genotype'][annot_table['assay'] == i]))
		numnf2 = len(set(annot_table['nf2Genotype'][annot_table['assay'] == i]))
		numtumor = len(set(annot_table['tumorType'][annot_table['assay'] == i]))

	if assaysNumSynId.get(i) is None:
		assaysNumSynId[i] = numAssay
	else:
		assaysNumSynId[i] = assaysNumSynId[i] + numAssay
	if assaysNF1genotype.get(i) is None:
		assaysNF1genotype[i] = numnf1
	else:
		assaysNF1genotype[i] = assaysNF1genotype[i] + numnf1
	if assaysNF2genotype.get(i) is None:
		assaysNF2genotype[i] = numnf2
	else:
		assaysNF2genotype[i] = assaysNF2genotype[i] + numnf2
	if assaysTumorType.get(i) is None:
		assaysTumorType[i] = numtumor
	else:
		assaysTumorType[i] = assaysTumorType[i] + numtumor

temp=pd.DataFrame()
for i in assaysNumSynId.keys():
	newValues = {'assay':str(i), 'numberOfFiles':assaysNumSynId[i], 'numberOfNF1Genotype':assaysNF1genotype[i], "numberOfNF2Genotype":assaysNF2genotype[i], "numberOfTumorTypes":assaysTumorType[i]}
	temp = temp.append(pd.DataFrame(newValues, index=[0]))
print(temp.to_csv(sep="|",index=False))
