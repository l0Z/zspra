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
        self.Paths={}# (sid,tid):{path,count}
        self.MAXLEN=10
        self.querynodes
        self.clicknodes

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
        paths=getpath(pre, TargetID)
        return paths
    def countpath2(self,pre,TargetID):
        '''
        pre[inode]=[nodelist1,nodelist2,...
        '''
        pathcache={}# pathcache[inode]=[]  {path:count}
        def getpath(pre,inode):
            if pathcache.has_key(inode):
                return pathcache[inode]
            paths={}
            if not pre[inode]:
                #inode is root
                return {}
            for (prenodes,prepath) in pre[inode]:
                if not prepath:
                    #prenodes
                    paths[preedge]=paths.get(preedge,0)+1

                for ipath,icount in prepaths:
                    npath=ipath+'|'+preedge
                    paths[npath]=paths.get(npath,0)+icount
#                 paths.extend([ i.append(preedge) for i in prepaths] )
            pathcache[inode]=paths
            return paths
        paths=getpath(pre, TargetID)
        return pa
    def bfs(self,SourceID):
        paths={}
        queue=[]
        queue.append(([],SourceID))
        while queue:
            ipath,inode=queue.pop(0)
            if len(ipath)>self.MAXLEN:
                return
            if isclick(inode):
                paths[inode]=paths.get(inode,[]).append(ipath)

            for inb in self.AdjacencyList[inode].iterkeys():
                if inb not in ipath:
                    queue.append((ipath.append(inode),inb))
        return paths

    def FindPaths(self,maxnum):
        '''
        for each source node in query, do bfs and turn paths into pathtypes.
        then calcu pathcounts
        '''
        SourceNodes=self.querynodes
        pathcounts={}
        for inode in SourceNodes:
            paths=self.bfs(inode)
            for itarget,ipaths in paths.iteritems():
                for ipath in ipaths:
                    ipathtype=self.path2type(ipath.append(itarget))
                    pathcounts[ipathtype]=pathcounts.get(ipathtype,0)+1
        watchdict(pathcounts,maxnum)
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
                for iprenodes in pre[inb]:

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

