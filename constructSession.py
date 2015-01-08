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
import numpy as np
def watchdict(d):
    '''
    top k item,
    mean,median of item frequency
    '''
    logger=logging.getLogger(__name__)
    sd=sorted(d.items(),key=lambda x:x[1],reverse=True)
    logger.info('top k %s',sd[:100])
    logger.info('mean %s',np.mean([i[1] for i in sd]))
    logger.info('median %s',np.median([i[1] for i in sd]))
    
    
def dealwithsessions(sessions):
    #frequency dict
    querycount={}
    phrasecount={}
    clickcount={}
    for isession in sessions:
        for q in isession.querylist:
            querycount[q[0]]=querycount.get(q,0)+1
        for iurl in isession.urls:
            clickcount[iurl]=clickcount.get(iurl,0)+1
        for iw in isession.spots:
            phrasecount[iw]=phrasecount.get(iw,0)+1
        for iw in  isession.refiners:
            phrasecount[iw]=phrasecount.get(iw,0)+1
    #logging
    
    return querycount,phrasecount,clickcount
            
            
    
        
        
        
        



