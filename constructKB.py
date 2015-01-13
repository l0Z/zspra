#coding:utf-8
'''
Created on 2015年1月3日

@author: ZS


'''
import cPickle as pickle
import logging,sys
import numpy as np
import time
negelecttype=set(['user','type','base','comm','free','symb'])
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

def findneighbours(edgecounts,topicid,response):
    neighbours={}
    names={}
    def doineighbor(iproperty,ineighbors):  
        if iproperty[1:5] in negelecttype and iproperty!='/common/topic/notable_for':
            return 
        if not edgecounts.has_key(iproperty):
            edgecounts[iproperty]=1
        else:
            edgecounts[iproperty]=1+edgecounts[iproperty]
            
        for ineighbor in ineighbors['values']:
            if ineighbor.has_key('id') and ineighbor.has_key('text'):
                neighbours[ineighbor['id']]=iproperty       
                names[ineighbor['id']]=ineighbor['text']
        return
    #print response
    #if response['property'].has_key('/common/topic/notable_for'
    for iproperty,ineighbors in response['property'].iteritems():
        if iproperty[1:5] in negelecttype and iproperty!='/common/topic/notable_for':
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
    for count,(mid,response) in enumerate(topiccache): 
        if not count%3000:
            logger=logging.getLogger(__name__)
            logger.info('len of totalmids %s',len(fbmids))
            logger.info('len of totalurls %s',len(fburls))
     #       pickle.dump(fbmids, open(idir+'/fbmids.pkl','wb'))
      #      pickle.dump(fburls, open(idir+'/fburls.pkl','wb'))
            pickle.dump(nbcache, open(idir+'/nbcache15.pkl','wb'))
        neighbours,names=findneighbours(edgecounts, mid, response)
#         print edgecounts
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
    watchdict(edgecounts,50)
    pickle.dump(fbmids, open(idir+'/fbmids15.pkl','wb'))
    pickle.dump(nbcache, open(idir+'/nbcache15.pkl','wb'))
    pickle.dump(edgecounts, open(idir+'/edgecounts.pkl','wb'))
def nearbyentity(coverentity,nbcache):
    '''
    coverentity: set
    '''
#     def u(s1,s2):
#         return s1|s2
    nbs=[set(nbcache.get(mid,{}).keys()) for mid in coverentity]
    print 'nbs get'
    return coverentity| set([j for i in nbs for j in i])
def testnb():
    coverentity=set([1,2])
    nbcache={1:{2:3,3:4}}
    print nearbyentity(coverentity, nbcache)

def mergedict(dname):
    mdir='/home/zhaoshi/文档/topicdata/topicfb'
    nd={}
    for i in xrange(15):
        idir=mdir+str(i)+'/'+dname+'.pkl'
        idict=pickle.load(open(idir,'rb'))
        if type(idict.values()[0])==int:
            for item,value in idict.iteritems():
                nd[item]=nd.get(item,0)+float(value)
        else:
            nd.update(idict)

    pickle.dump(nd, open('/home/zhaoshi/文档/topicdata/'+dname+'.pkl','wb'))
    watchdict(nd, 100)
    return nd 
def kb2graph(f,edged,entityd,nbcache):
    logger=logging.getLogger(__name__)
    for ientity,inbs in nbcache.iteritems():
        eid=entityd.get(ientity,-1)
        for inb,iedge in inbs.iteritems():
            nbid=entityd.get(inb,-1)
            edgeid=edged.get(iedge,-1)
            if eid!=-1 and nbid!=-1 and edgeid!=-1:
                f.write(str(eid)+' '+str(nbid)+' '+str(edgeid)+'\n')
            else:
                logger.info('false %s %s %s',ientity,inb,iedge)
                
            
    
def test_kbgraph():
    nbcache=pickle.load(open('/home/zhaoshi/文档/topicdata/nbcache15.pkl','rb'))
    edgecount= pickle.load(open('/home/zhaoshi/文档/topicdata/edgecounts.pkl','rb')) 
    edged=dict([(edge,i+5) for(i,edge) in enumerate(edgecount.keys()) ] )   
    entities=pickle.load(open('/home/zhaoshi/文档/topicdata/nearbyentities.pkl','rb')) ##set
    entityd=dict([ (mid,i) for i,mid in enumerate(entities)]) 
    pickle.dump(edged, open('/home/zhaoshi/文档/topicdata/edged','wb'))
    pickle.dump(entityd, open('/home/zhaoshi/文档/topicdata/entityd','wb'))
    f=open('kbgraph.txt','wb')
    kb2graph(f, edged, entityd, nbcache)
    f.close()


   
def test1():
    for idict in ['nbcache15','fbmids15','edgecounts',]:
        mergedict(idict)      
def watch():
    #watch edgecount
    edgecount=pickle.load(open('/home/zhaoshi/文档/topicdata/edgecounts.pkl','rb'))   
    print len(edgecount)
    watchdict(edgecount, 1000)       
def findnearby():
    coverentity=pickle.load(open('/home/zhaoshi/文档/nodelibrary/coveredmids.pkl','rb'))
    nbcache=pickle.load(open('/home/zhaoshi/文档/topicdata/nbcache15.pkl','rb'))
    entities=nearbyentity(set(coverentity.keys()),nbcache)
    print len(entities)
    pickle.dump(entities, open('/home/zhaoshi/文档/topicdata/nearbyentities.pkl','wb'))
    
def test():
#     dealtopic('/home/zhaoshi/文档/topicdata/topicfb0')
#     dealtopic('/Users/ZS/Documents/workspace/AOLlog')
#     sys.argv.append('/Users/ZS/Documents/workspace/AOLlog')
    logging.basicConfig(filename=sys.argv[1]+'/constructKBlogging.txt',level=logging.INFO)
    dealtopic(sys.argv[1])
   # dealtopic(sys.argv[1])
   # dirlist=['/home/zhaoshi/文档/topicdata/topicfb0','/home/zhaoshi/文档/topicdata/topicfb1','/home/zhaoshi/文档/topicdata/topicfb2','/home/zhaoshi/文档/topicdata/topicfb3','/home/zhaoshi/文档/topicdata/topicfb4','/home/zhaoshi/文档/topicdata/topicfb5','/home/zhaoshi/文档/topicdata/topicfb6','/home/zhaoshi/文档/topicdata/topicfb7','/home/zhaoshi/文档/topicdata/topicfb8','/home/zhaoshi/文档/topicdata/topicfb9','/home/zhaoshi/文档/topicdata/topicfb10','/home/zhaoshi/文档/topicdata/topicfb11','/home/zhaoshi/文档/topicdata/topicfb12','/home/zhaoshi/文档/topicdata/topicfb13','/home/zhaoshi/文档/topicdata/topicfb14']
    #for idir in dirlist:
     #   ifile=idir+'/topiccache.pkl'
     #   dealtopic(ifile)
if __name__=='__main__':
    logging.basicConfig(filename='mergelogging.txt',level=logging.INFO)
    test_kbgraph()
#     watch()
#     findnearby()
#     testnb()

#     test1()
#     import json