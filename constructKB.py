#coding:utf-8
'''
Created on 2015年1月3日

@author: ZS


'''
import cPickle as pickle
import logging,sys
negelecttype=['user','type','base','comm','free','symb']
def findneighbours(edgecounts,topicid,response):
    neighbours={}
    names={}
    def doineighbor(iproperty,ineighbors):   
        if not edgecounts.has_key(iproperty):
            edgecounts[iproperty]=1
        else:
            edgecounts[iproperty]=1+edgecounts[iproperty]
            
        for ineighbor in ineighbors['values']:
            if ineighbor.has_key('id') and ineighbor.has_key('text'):
                neighbours[ineighbor['id']]=iproperty       
                names[ineighbor['id']]=ineighbor['text']
    #print response
    #if response['property'].has_key('/common/topic/notable_for'
    for iproperty,ineighbors in response['property'].iteritems():
        if iproperty[1:5] in negelecttype:
            if iproperty!='/common/topic/notable_for':
                continue
        if ineighbors.get('valuetype',0)=='compound':
            for iv in ineighbors['values']:
                for ip,inn in iv['property'].iteritems():
                    if inn.get('valuetype',0)=='object':
                        doineighbor(ip, inn)
        elif ineighbors.get('valuetype',0)=='object':
            doineighbor(iproperty, ineighbors)
        else:
            pass
        #print ineighbors           
    return neighbours,names
#t,n=findneighbours('m/02jx1',requests.get('https://www.googleapis.com/freebase/v1/topic/m/02jx1?key=AIzaSyCy2laLO2b3hnU_uXoEJ6JEST9aapBzduk'').json())
def dealtopic(idir):
    topicdir=idir+'/topiccache.pkl'
    fbmids={}
    topiccache=pickle.load(open(topicdir,'rb'))
    fburls={}
    nbcache={}
    edgecounts={}
    logging.basicConfig(file=idir+'/constructKBlogging.txt',level=logging.INFO)
    for count,(mid,response) in enumerate(topiccache): 
        if not count%3000:
            logger=logging.getLogger(__name__)
            logger.info('len of totalmids %s',len(fbmids))
            logger.info('len of totalurls %s',len(fburls))
     #       pickle.dump(fbmids, open(idir+'/fbmids.pkl','wb'))
      #      pickle.dump(fburls, open(idir+'/fburls.pkl','wb'))
            pickle.dump(nbcache, open(idir+'/nbcache15.pkl','wb'))
        neighbours,names=findneighbours(edgecounts, mid, response)
        print neighbours,names
        if neighbours:      
            nbcache[mid]=neighbours
            fbmids.update(names)
#         if response['property'].has_key('/common/topic/official_website'):
#             for i in response['property']['/common/topic/official_website']['values']:
#                  fburls[i['text']]=mid
#         for neighbor in response['property'].values():
#             for i in neighbor['values']:
#                 if i.has_key('id') and i.has_key('text'):
#                     fbmids[i['id']]=i['text']  
                    
    logger=logging.getLogger(__name__)
    logger.info('len of totalmids %s',len(fbmids))
    logger.info('len of totalurls %s',len(fburls))
    pickle.dump(fbmids, open(idir+'/fbmids15.pkl','wb'))
    pickle.dump(nbcache, open(idir+'/nbcache15.pkl','wb'))
def nearbyentity(coverentity,nbcache):
    '''
    coverentity: set
    '''
    def u(s1,s2):
        return s1|s2
    return coverentity| reduce(u, [set(nbcache[mid].keys()) for mid in coverentity])
    

def test():
    dealtopic('/home/zhaoshi/文档/topicdata/topicfb0')
#     dealtopic(sys.argv[1])
   # dealtopic(sys.argv[1])
   # dirlist=['/home/zhaoshi/文档/topicdata/topicfb0','/home/zhaoshi/文档/topicdata/topicfb1','/home/zhaoshi/文档/topicdata/topicfb2','/home/zhaoshi/文档/topicdata/topicfb3','/home/zhaoshi/文档/topicdata/topicfb4','/home/zhaoshi/文档/topicdata/topicfb5','/home/zhaoshi/文档/topicdata/topicfb6','/home/zhaoshi/文档/topicdata/topicfb7','/home/zhaoshi/文档/topicdata/topicfb8','/home/zhaoshi/文档/topicdata/topicfb9','/home/zhaoshi/文档/topicdata/topicfb10','/home/zhaoshi/文档/topicdata/topicfb11','/home/zhaoshi/文档/topicdata/topicfb12','/home/zhaoshi/文档/topicdata/topicfb13','/home/zhaoshi/文档/topicdata/topicfb14']
    #for idir in dirlist:
     #   ifile=idir+'/topiccache.pkl'
     #   dealtopic(ifile)
if __name__=='__main__':
    test()
#     import json