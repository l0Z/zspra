#coding:utf-8
'''
Created on 2015年1月6日
TEST

@author: ZS
'''
#first nodedict
#phrasecount
import logging
import numpy as np
import time
import cPickle as pickle
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
def isclick(nodeid):
    #
    return True
class pragraph():
    def __init__(self,):
        self.AdjacencyList={} #sid:{tid:(edge,w)}
        self.paths={}# (sid,tid):{path,count}
        self.pathtypes={}
        self.bfscache={}
        self.MAXLEN=10
        self.qnum=82070
        self.pnum=84160
        self.cnum=63019
        self.enum=896667
#         self.querynodes
#         self.clicknodes
    def text2graph(self,f):
        '''convert text to graph :  construct adjacencylist'''
        # graph text: each line an edge, ntype+nid+edgeid+ntype+nid+edgeweight
        for iline in f.readlines():
            iline=iline.split()
            n1=iline[0]
            n2=iline[2]
            edge=int(iline[1])
            if len(iline)>3:
                w=float(iline[3])
            else:
                w=1
            if not self.AdjacencyList.has_key(n1):
                self.AdjacencyList[n1]={}
            if not self.AdjacencyList.has_key(n2):
                self.AdjacencyList[n2]={}
            self.AdjacencyList[n1][n2]=[(edge,w),]
            self.AdjacencyList[n2][n1]=[(-edge,w),]
        
            
        
    def path2type(self,path):
        '''
        turn Path(nodelist) to pathtype string
        '''

        edges=[self.AdjacencyList[ path[i] ][ path[i+1] ][0] for i in xrange(len(path))]
        return ' '.join(edges)

    def countpath(self,pre):
        '''
        pre[inode]=[(prenode,preedge),……]
        turn prenodes'trace into paths and count them

        store path in a list and count them at the last step
        or store in a dict but then can't use list to store path
        '''
        pathcache={}# pathcache[inode]=[]  {path:count}
        def getpath(pre,inode):
            if pathcache.has_key(inode):
                return pathcache[inode]
            paths={}
            for (prenode,preedge) in pre[inode]:
                if not preedge:
                    return {}
                prepaths=getpath(pre,prenode)
                if not prepaths:
                    paths[preedge]=paths.get(preedge,0)+1

                for ipath,icount in prepaths:
                    npath=ipath+'|'+preedge
                    paths[npath]=paths.get(npath,0)+icount
#                 paths.extend([ i.append(preedge) for i in prepaths] )
            pathcache[inode]=paths
            return paths
#         paths=getpath(pre, TargetID)
    def countpath2(self,pre,TargetID):
        '''
        pre[inode]=[nodelist1,nodelist2,...
        '''
#         pathcache={}# pathcache[inode]=[]  {path:count}
#         def getpath(pre,inode):
#             if pathcache.has_key(inode):
#                 return pathcache[inode]
#             paths={}
#             if not pre[inode]:
#                 #inode is root
#                 return {}
#             for (prenodes,prepath) in pre[inode]:
#                 if not prepath:
#                     #prenodes
#                     paths[preedge]=paths.get(preedge,0)+1
# 
#                 for ipath,icount in prepaths:
#                     npath=ipath+'|'+preedge
#                     paths[npath]=paths.get(npath,0)+icount
# #                 paths.extend([ i.append(preedge) for i in prepaths] )
#             pathcache[inode]=paths
#             return paths
#         paths=getpath(pre, TargetID)
#         return paths

    def randomwalk(self,SourceID,pathtype):
        return
        #return TargetID and
        #if pathtype is null, collect 
        
#这不能化成DFS子问题，因为只有给定的源点到目标点有路径限制，中间的子问题则无法判断路径限制    
#     def dfsbeifen(self,Sources, Targets):
#         paths={}# paths[(s,t)]=[a node series]
#         for isnode in Sources:
#             for inb in self.AdjacencyList[isnode].iterkeys():
#                 if not paths.has_key((isnode,inb))
#                     paths[(isnode,inb)]=
#                 paths[(isnode,inb)]=dfs
#                 if inb not in ipath:
#                     queue.append((ipath.append(inode),inb))
    def dfs(self,SourceID, TargetID,MaxLen):
        #只存中继节点到目的节点的序列，Maxlen最低为1，即在邻居内可以找到目的节点
        if self.paths.has_key((SourceID, TargetID)):
            return self.paths[(SourceID, TargetID)]
        if SourceID==TargetID:
            return [TargetID,]
        if MaxLen<1:
            return []

        for inb in self.AdjacencyList[SourceID].iterkeys():
            nb2tpaths=self.dfs(inb,TargetID,MaxLen-1)
            if not nb2tpaths:
                self.paths[(SourceID, TargetID)]=[[inb,]+ipath for ipath in nb2tpaths ]
        return self.paths[(SourceID, TargetID)]
    
    def FindPaths_dfs(self):
        for i in xrange(self.qnum):
            for j in xrange(self.cnum):
                self.dfs('q'+str(i),'c'+str(j),self.MAXLEN)
                
        pickle.dump(self.paths, open('paths.pkl'), protocol=2)
        
    
    def bfs(self,SourceID):
        '''
        paths starting from 
        path是左闭右开区间
        其实这个设置缓存子问题也是没用的，因为每个子问题中，哪些点能用到的限制也不同
        '''
        logger=logging.getLogger(__name__)
        if self.bfscache.has_key(SourceID):
            return 
        paths={}
        queue=[]
        t1=time.clock()
        queue.append(([],SourceID))
        ilevel=0
        while queue:
            ipath,inode=queue.pop(0)
            enum=len([ i for i in ipath if i[0]=='e'])
            if enum>3:
                return
            currentlevel=len(ipath)
            if currentlevel>self.MAXLEN:
                return
            if currentlevel>ilevel:
                ilevel=currentlevel
                print 'ilevel',ilevel
#             if isclick(inode):
            if inode[0]=='q'and self.bfscache.has_key(inode) :
#                 self.bfs(inode)
                for itarget,ipaths in self.bfscache[inode].iteritems():
                    paths[itarget]=[ipath+iipath for iipath in ipaths]
                
            if inode[0]=='c':
#                 paths[inode]=paths.get(inode,[]).append(ipath)
                paths[inode]=paths.get(inode,[])
                paths[inode].append(ipath)
                logger.info('inode %s  paths  %s ',inode,paths) 
            for inb in self.AdjacencyList[inode].iterkeys():
                if inb not in ipath:
                    queue.append((ipath+[inode,],inb))
        logger.info('sourceID %s arrive at %s clicks with %s paths in %s time',SourceID,len(paths),sum( [len(i) for i in paths.itervalues()]),time.clock()-t1 ) 
        self.bfscache[SourceID]=paths
        return 

    def FindPaths_bfs(self):
        '''
        for each source node in query, do bfs and turn paths into pathtypes.
        then calcu pathcounts
        '''
#         self.qnum=82070
        trainpairs=pickle.load(open('pairs.pkl','rb'))
        queries=[i[0] for i in trainpairs]
        self.bfscache={}
        pathcounts={}
#         for i in xrange(self.qnum):
#             inode='q'+str(i)
        for inode in queries:
            print inode,len(pathcounts)
            self.bfs(inode)
            print 'bfs done'
            for itarget,ipaths in self.bfscache[inode].iteritems():
                for ipath in ipaths:
                    ipath.append(itarget)
                    ipathtype=self.path2type(ipath)
                    pathcounts[ipathtype]=pathcounts.get(ipathtype,0)+1
        watchdict(pathcounts,1000)
        self.pathtypes=pathcounts
        return pathcounts
        #
    def bfsbeifen(self,SourceID,TargetID):
        queue=[]
        queue.append(SourceID)
#         queue.append((None,SourceID,0))
        curnodes=set([])
        pathcounts={}
        pre={} #inode:[nodelist1,nodelist2...]
        # e.g. pre[3]=[[012],[02]...] pre[0]=[] pre[1]=[[0],], pre[2]=[pre[0]+0,pre[1]+1]=[[0],[0,1]]
        pre[SourceID]=[]

        while not queue:
            inode=queue.pop()

            if len(curnodes)>self.MAXLEN:
                return
#             curpath.append(inedge)

            if inode==TargetID:
#                 npath=curpath+'|'+inedge
#                 pathcounts[curpath]=pathcounts.get(curpath,0)+1
#                 pathcounts[npath]=pathcounts.get(npath,0)+1
                #backtrace^not change curpath and curnodes
                return
#             curpath=curpath+'|'+inedge
#             curnodes.add(inode)

            #visit node~

            for inb in self.AdjacencyList[inode].iterkeys():
#                 for iprenodes in pre[inb]:

                if inb not in curnodes:
                    queue.append(inb)
                    pre[inode]=pre.get(inode,[]).extend([i.append(inb) for i in pre[inb]])
                    self.dfs()


    def FindPathsbefien(self,SourceID,TargetID):
        '''DFS+DP'''
        logger=logging.getLogger(__name__)
        logger.info('s %s, t %s, Pathcount %s',SourceID,TargetID,len(self.Paths))
        if self.Paths.has_key((SourceID,TargetID)):
            return self.Paths[(SourceID,TargetID)]
        if self.AdjacencyList[SourceID].has_key(TargetID):
            return {self.AdjacencyList[SourceID][TargetID][0]:1,}
        pathcounts={}
        visitednodes=set([])
        for inb,(iedge,iw) in self.AdjacencyList[SourceID].iteritems():
            if not self.Paths.has_key((inb,TargetID)):
                self.FindPaths(inb,TargetID)
            for ipath,count in self.Paths[inb,TargetID].iteritems():
                pathcounts[iedge+'|'+ipath]=count
        return pathcounts





    def CountPaths(self,SourceNodes,TargetNodes):
        '''
        path stored as 'edgeid|edgeid|edgeid'
        random walk on the graph to count paths
        '''
        Paths={}
        #Paths[(sID,tID)]={path:count}

def test_constructgraph():
    zspra=pragraph()
    f=open('/home/zhaoshi/文档/nodelibrary/sigir15/zsgraph.txt')
    zspra.text2graph(f)
    f.close()
    pickle.dump(zspra,open('pragraph.pkl','wb'),protocol=2)

def test_findpaths():
    import sys
    sys.setrecursionlimit(1000000) 
    logging.basicConfig(filename='bfslogging.txt',level=logging.INFO)
    zspra=pickle.load( open('pragraph.pkl','rb'))
    zspra.FindPaths_bfs()
    pickle.dump(zspra,open('bfs_pragraph_115.pkl','wb'),protocol=2)
    
if __name__=='__main__':
    import time
    t=time.clock()
#     test_constructgraph()
    test_findpaths()
    print time.clock()-t
    
    