#coding:utf-8
'''
Created on 2015年1月5日

@author: ZS

construct graph from sessions
 newsession510.pkl
 querylist 
 spot 
 
'''
import logging
import cPickle as pickle
import numpy as np

def watchdict(d,k):
    '''
    top k item,
    mean,median of item frequency
    '''
    logger=logging.getLogger(__name__)
    sd=sorted(d.items(),key=lambda x:x[1],reverse=True)
    logger.info('top k %s',sd[:k])
    logger.info('mean %s',np.mean([i[1] for i in sd]))
    logger.info('median %s',np.median([i[1] for i in sd]))
# def watchdict(d):
#     '''
#     top k item,
#     mean,median of item frequency
#     '''
#     logger=logging.getLogger(__name__)
#     sd=sorted(d.items(),key=lambda x:x[1],reverse=True)
#     logger.info('top k %s',sd[:100])
#     logger.info('mean %s',np.mean([i[1] for i in sd]))
#     logger.info('median %s',np.median([i[1] for i in sd]))
    
    
def dealwithsessions(sessions):
    #frequency dict
    querycount={}
    phrasecount={}
    clickcount={}
    coveredmids={}
    for icount,isession in enumerate(sessions):
        if not icount%5000:
            logger=logging.getLogger(__name__)
            logger.info('len of querycount %s phrasecount%s clickcount%s',len(querycount),len(phrasecount),len(clickcount))
            logger.info('len of coveredmids %s',len(coveredmids))
        for q in isession.querylist:
            querycount[q[0]]=querycount.get(q[0],0)+1
        for iurl in isession.urls:
            clickcount[iurl]=clickcount.get(iurl,0)+1
        for iw in isession.spots:
            phrasecount[iw]=phrasecount.get(iw,0)+1
        for iw in  isession.refiners:
            phrasecount[iw]=phrasecount.get(iw,0)+1
        for ispot,itopics in isession.topicpath.iteritems():
            for itopic,iw in itopics.iteritems():
                coveredmids[itopic]=coveredmids.get(itopic,0)+iw
        
    #logging
    k=100
    watchdict(querycount, k)
    pickle.dump(querycount, open( 'querycount.pkl','wb'))
    watchdict(phrasecount, k)
    pickle.dump(phrasecount, open( 'phrasecount.pkl','wb'))
    watchdict(clickcount, k)
    pickle.dump(clickcount, open( 'clickcount.pkl','wb'))
    watchdict(coveredmids, k)
    pickle.dump(coveredmids, open( 'coveredmids.pkl','wb'))
    return querycount,phrasecount,clickcount,coveredmids
def log2graph(f,sessions,queryd,phrased,urld,entityd):
    #eid 0~N ,qid N~, urlid~ ,phrased ~
    #start with e,q,c ,p
#     entityN=len(entityd)
#     queryN=len(queryd)
#     urlN=len(urld)
#     phraseN=len(phrased)
#     
    logger=logging.getLogger(__name__)
    for isession in sessions:
        for iq in isession.querylist:
            qid=queryd.get(iq[0],-1)
            urlid=urld.get(iq[2][7:],-1)
            if qid==-1:
                continue
            if  urlid!=-1:
                #query has click
                logger.info('q %s click c %s',iq[0],iq[2][7:])
                
                f.write('q'+str(qid)+' 1 '+'c'+str(urlid)+'\n')
#                 f.write(str(qid+entityN)+' 1 '+str(urlid+queryN+entityN)+'\n')
            else:
                print 'wrong url',iq[2]
                
            if len(iq)>3:
                for ispot in set(iq[3]+iq[4]):
                    spotid=phrased.get(ispot.strip(),-1)
                    ##query has spot
                    if spotid!=-1:
                        f.write('q'+str(qid)+' 2 '+'p'+str(spotid)+'\n')
#                         f.write(str(qid+entityN)+' 2 '+str(spotid+queryN+entityN+phraseN)+'\n')
            else:
                #the whole query as refiner
                refiners=set( iq[0].split()+[iq[0],] )
                for ire in refiners:
                    pid=phrased.get(ire,-1)
                    if pid!=-1:
                        f.write('q'+str(qid)+' 2 '+'p'+str(pid)+'\n')
                
#         for inb in isession.topicpath:
        for ispot,itopics in isession.topicpath.iteritems():
            for itopic,iw in itopics.iteritems():
                spotid=phrased.get(ispot,-1)
                topicid=entityd.get(itopic,-1)
                #spot map to topic
                if spotid!=-1 and topicid!=-1:
                    f.write('p'+str(spotid)+' 3 '+'e'+str(topicid)+' '+str(iw)+'\n')
def count2dict(mydir):   
    queryc=pickle.load(open( mydir,'rb'))
    queryd=[i for i in queryc if queryc[i]>1  ]
    queryd=dict( [(iquery,i) for i,iquery in enumerate(queryd ) ] )         
    return queryd       
def train_pairs():
    f=open('loggraph.txt','rb')
    trainpairs=[]
    for i in f.readlines():
        line=i.split() 
        if line[1]=='1':
            print line       
        if line[0][0]=='q' and line[2][0]=='c':
            trainpairs.append((line[0],line[2]))
    print len(trainpairs)
    trainpairs=set(trainpairs)
    print len(trainpairs)
    pickle.dump(trainpairs, open('pairs.pkl','wb'), protocol=2)
             
            
def test_sessiongraph():
    sessions=pickle.load(open('/home/zhaoshi/文档/newsession510.pkl','rb'))
    entityd=pickle.load(open('/home/zhaoshi/文档/topicdata/entityd','rb'))
    queryd=count2dict( 'cut_querycount.pkl')
    phrased=count2dict('cut_phrasecount.pkl')
    urld=count2dict('clickcount.pkl')
    f=open('loggraph.txt','wb')
    log2graph(f,sessions,queryd,phrased,urld,entityd)
    f.close()
    
            
def test():
    sessions=pickle.load(open('/home/zhaoshi/文档/newsession510.pkl','rb'))
    logging.basicConfig(filename='watchpairslogging.txt',level=logging.INFO)
#     logging.basicConfig(filename='constructSessionlogging.txt',level=logging.INFO)
#     logging.basicConfig(filename='constructSessionlogging.txt',level=logging.INFO)
    dealwithsessions(sessions)

if __name__=='__main__':
#     test_sessiongraph()            
#     train_pairs()
    test()
    
        
        
        
        



