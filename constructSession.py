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
from constructKB import watchdict
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
            querycount[q[0]]=querycount.get(q,0)+1
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
    watchdict(phrasecount, k)
    watchdict(clickcount, k)
    watchdict(coveredmids, k)
    return querycount,phrasecount,clickcount,coveredmids
            
def test():
    sessions=pickle.load(open('/home/zhaoshi/文档/newsession510.pkl','rb'))
    
    logging.basicConfig(filename='constructSessionlogging.txt',level=logging.INFO)
    dealwithsessions(sessions)

if __name__=='__main__':
    test()            
    
        
        
        
        



