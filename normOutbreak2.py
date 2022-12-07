#!/software/bin/python3 -u

import pandas as pd

tx = pd.read_csv('NCBItaxNames.csv')
ptx = pd.read_csv('NCBItaxNamesPathogen.csv')
empres = pd.read_csv('EMPRES-geo-1000-by-species.tsv',sep=chr(9))

ptx.ptgn_tax_id=ptx.ptgn_tax_id.astype('str')
tx.tax_id=tx.tax_id.astype('str')

empres['species']=empres['Species'].str.lower()
empres['serotype']=empres.Serotype.str.replace(' \S+$','')

x=pd.merge(empres,tx,how='left',left_on='species',right_on='synonym_name')
x=pd.merge(x,ptx,how='left',left_on='serotype',right_on='ptgn_synonym_name')


out=pd.DataFrame()
out['Date observed']=pd.to_datetime(x['Observation.date..dd.mm.yyyy.'],infer_datetime_format=True)
out['Date reported']=pd.to_datetime(x['Report.date..dd.mm.yyyy.'],infer_datetime_format=True)
out['Pathogen species ']=x.ptgn_sciname
out['Pathogen serotype']=x.serotype
out['Pathogen NCBI taxonomy ID']=x.ptgn_tax_id #.astype('Int64')
out['Host species Latin name']=x.sciname
out['Host species NCBI taxonomy ID']=x.tax_id #.astype('Int64')
out['Host species common name']=x.common_name
out['Animal domestication status']=x['Animal.type']
out['Latitude']=x['Latitude']
out['Longitude']=x['Longitude']
out['Geo text original ']=x['Original location']
out['Country OSM ID']=x['Country OSM ID']
out['Admin. level 1 OSM ID']=x['Admin. level 1 OSM ID']
out['Locality OSM ID']=x['Locality OSM ID']
out['Data source']=x['Diagnosis.source']
out['Data source']=x['Diagnosis.source']
out['Original record ID']=x['Event.ID']

out.to_csv('out.tsv',sep=chr(9))
