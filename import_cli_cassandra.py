#encoding=utf-8
import pandas as pd
import glob
import os
from cassandra.cluster import Cluster

cluster = Cluster(['192.168.1.66'])
session = cluster.connect('clichn') ## USE mykeyspace;
#path = 'D:/data/test'
path = u'D:/data/tem'
filelist = glob.glob(os.path.join(path,'*.txt'))
cqlstr = 'INSERT INTO clichn.tem (station, date, longitude, latitude, elevation, year, month, day, tem_avg, tem_max, tem_min, qc_avg, qc_max, qc_min) \
VALUES( %s,"%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s );'
cqlstr = '''
INSERT INTO clichn.tem (station, date, longitude, latitude, elevation, year, month, day, tem_avg, tem_max, tem_min, qc_avg, qc_max, qc_min)
VALUES( %s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s );
'''
query  = '''
    INSERT INTO clichn.tem (station,date,longitude, latitude, elevation, year, month, day, tem_avg, tem_max, tem_min, qc_avg, qc_max, qc_min)
VALUES( ?,?,?,?,?,?,?,?,?,?,?,?,?,? );
'''
prepared = session.prepare(query)


batch_size = 200 #200,    (1000 too large)
insert_stmt = query

i=0
for filename in filelist:
    tags = filename.split('-')
    if len(tags)>2:
        if tags[1]=='TEM':        
            cols =['station','longitude','latitude','elevation','year','month','day',\
                           'tem_avg','tem_max','tem_min','qc_avg','qc_max','qc_min']
            col_widths = [5,5,6,7,5,3,3,7,7,7,2,2,2]

            print '----', filename, '----'
            dataset = pd.read_fwf(filename,widths=col_widths, names=cols)#parse_dates={'date':['year','month','day']}
                           
            batch_stmt = 'BEGIN UNLOGGED BATCH '
            for id in dataset.index:
                ### BATCH
                date = '%s-%s-%s 00:00:00-0800'%(dataset.ix[id,'year'], dataset.ix[id,'month'], dataset.ix[id,'day'])
                batch_stmt += cqlstr %(dataset.ix[id,'station'], date, dataset.ix[id,'longitude'], dataset.ix[id,'latitude'], \
                dataset.ix[id,'elevation'], dataset.ix[id,'year'], dataset.ix[id,'month'], dataset.ix[id,'day'], \
                dataset.ix[id,'tem_avg'], dataset.ix[id,'tem_max'], dataset.ix[id,'tem_min'], \
                dataset.ix[id,'qc_avg'], dataset.ix[id,'qc_max'], dataset.ix[id,'qc_min'] ) 
                '''print '--', dataset.ix[id,'station'], dataset.ix[id,'date'], dataset.ix[id,'longitude'], dataset.ix[id,'latitude'], \
                dataset.ix[id,'elevation'], dataset.ix[id,'date'].year, dataset.ix[id,'date'].month, dataset.ix[id,'date'].day, \
                dataset.ix[id,'tem_avg'], dataset.ix[id,'tem_max'], dataset.ix[id,'tem_min'], \
                dataset.ix[id,'qc_avg'], dataset.ix[id,'qc_max'], dataset.ix[id,'qc_min']   #'''
                i+=1
                if( (id+1)%batch_size==0 or id==(len(dataset.index)-1)):
                    batch_stmt += 'APPLY BATCH;'
                    ###print '--------------------'
                    ###print '%d    ', batch_stmt
                    pre_batch = session.execute(batch_stmt)  #session.prepare(batch_stmt)
                    batch_stmt = 'BEGIN BATCH '
                    print '%d    '%(i), dataset.ix[id,'station'], date
                ###=================================
                '''
                ### prepared statements
                bound_stmt = prepared.bind((dataset.ix[id,'station'], dataset.ix[id,'date'], dataset.ix[id,'longitude'], dataset.ix[id,'latitude'], \
                                dataset.ix[id,'elevation'], dataset.ix[id,'date'].year, dataset.ix[id,'date'].month, dataset.ix[id,'date'].day, \
                                dataset.ix[id,'tem_avg'], dataset.ix[id,'tem_max'], dataset.ix[id,'tem_min'], \
                                dataset.ix[id,'qc_avg'], dataset.ix[id,'qc_max'], dataset.ix[id,'qc_min'] ))
                session.execute(bound_stmt)
                '''
                ###=================================
                '''
                ### insert
                session.execute(cqlstr ,(dataset.ix[id,'station'], dataset.ix[id,'date'], dataset.ix[id,'longitude'], dataset.ix[id,'latitude'], \
                dataset.ix[id,'elevation'], dataset.ix[id,'date'].year, dataset.ix[id,'date'].month, dataset.ix[id,'date'].day, \
                dataset.ix[id,'tem_avg'], dataset.ix[id,'tem_max'], dataset.ix[id,'tem_min'], \
                dataset.ix[id,'qc_avg'], dataset.ix[id,'qc_max'], dataset.ix[id,'qc_min'] )  )
                '''
                '''
                ###
                print cqlstr % (dataset.ix[id,'station'], dataset.ix[id,'date'], dataset.ix[id,'longitude'], dataset.ix[id,'latitude'], \
                dataset.ix[id,'elevation'], dataset.ix[id,'date'].year, dataset.ix[id,'date'].month, dataset.ix[id,'date'].day, \
                dataset.ix[id,'tem_avg'], dataset.ix[id,'tem_max'], dataset.ix[id,'tem_min'], \
                dataset.ix[id,'qc_avg'], dataset.ix[id,'qc_max'], dataset.ix[id,'qc_min'] )
                '''

