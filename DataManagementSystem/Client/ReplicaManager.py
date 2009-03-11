""" This is the Replica Manager which links the functionalities of StorageElement and FileCatalog. """

__RCSID__ = "$Id: ReplicaManager.py,v 1.54 2009/03/11 20:06:01 acsmith Exp $"

import re, time, commands, random,os
import types

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.File import makeGuid,fileAdler
from DIRAC.Core.Utilities.File import getSize
from DIRAC.Core.Security.Misc import getProxyInfo,formatProxyInfoAsString

from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

class ReplicaManager:

  def __init__( self ):
    """ Constructor function.
    """
    self.fileCatalogue = FileCatalog()
    self.accountingClient = None
    self.registrationProtocol = 'SRM2'
    self.thirdPartyProtocols = ['SRM2']

  def setAccountingClient(self,client):
    """ Set Accounting Client instance
    """
    self.accountingClient = client

  def __getClientCertGroup(self):
    res = getProxyInfo(False,False)
    if not res['OK']:
      gLogger.error("ReplicaManager.__getClientCertGroup: Failed to get client proxy information.",res['Message'])  
      return res
    proxyInfo = res['Value']
    gLogger.debug(formatProxyInfoAsString(proxyInfo))
    if not proxyInfo.has_key('group'):
      errStr = "ReplicaManager.__getClientCertGroup: Proxy information does not contain the group."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    group = proxyInfo['group']
    username = proxyInfo['username']
    return S_OK(proxyInfo['group'])

  def __verifyClientPermissions(self,group,path):
    pass

  ##########################################################################
  #
  # These are the data transfer methods
  #

  def putDirectory(self,storagePath,localDirectory,diracSE):
    """ Put a local file to a Storage Element

        'lfn' is the path on the storage
        'localDirectory' is the full path to local directory
        'diracSE' is the Storage Element to which to put the file
    """
    # Check that the local directory exists
    if not os.path.exists(localDirectory):
      errStr = "ReplicaManager.putDirectory: Supplied directory does not exist."
      gLogger.error(errStr,localDirectory)
      return S_ERROR(errStr)
    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.put: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    destinationSE = storageElement.getStorageElementName()['Value']

    res = storageElement.getPfnForLfn(storagePath)
    if not res['OK']:
      errStr = "ReplicaManager.putDirectory: Failed to generate destination PFN."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    destPfn = res['Value']
    dirDict = {destPfn:localDirectory}

    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    putDirRes = storageElement.putDirectory(dirDict,singleDirectory=True)
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to put file to Storage Element."
      gLogger.error(errStr,"%s: %s" % (localDirectory,res['Message']))
    else:
      gLogger.info("ReplicaManager.put: Put directory to storage in %s seconds." % putTime)
    return res

  def getFile(self,lfn):
    """ Get a local copy of a LFN from Storage Elements.

        'lfn' is the logical file name for the desired file
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.getFile: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.getFile: Attempting to get %s files." % len(lfns))
    res = self.getReplicas(lfns)
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    lfnReplicas = res['Value']['Successful']
    res = self.getFileSize(lfnReplicas.keys())
    if not res['OK']:
      return res
    failed.update(res['Value']['Failed'])
    fileSizes = res['Value']['Successful']
    ###########################################################
    # Determine the best replicas
    replicaPreference = {}
    for lfn,size in fileSizes.items():
      replicas = []
      for diracSE,pfn in lfnReplicas[lfn].items():
        storageElement = StorageElement(diracSE)
        if storageElement.isValid()['Value']:
          local = storageElement.isLocalSE()['Value']
          fileTuple = (diracSE,pfn)
          if local:
            replicas.insert(0,fileTuple)
          else:
            replicas.append(fileTuple)
        else:
          errStr = "ReplicaManager.getFile: Failed to determine whether SE is local."
          gLogger.error(errStr,diracSE)
      if not replicas:
        errStr = "ReplicaManager.getFile: Failed to find any valid StorageElements."
        gLogger.error(errStr,lfn)
        failed[lfn] = errStr
      else:
        replicaPreference[lfn] = replicas
    ###########################################################
    # Get a local copy depending on replica preference
    successful = {}
    for lfn,replicas in replicaPreference.items():
      gotFile = False
      for diracSE,pfn in replicas:
        if not gotFile:
          storageElement = StorageElement(diracSE)
          res = storageElement.getFile(pfn,singleFile=True)
          if res['OK']:
            gotFile = True
            successful[lfn] = os.path.basename(lfn)
      if not gotFile:
        # If we get here then we failed to get any replicas
        errStr = "ReplicaManager.getFile: Failed to get local copy from any replicas."
        gLogger.error(errStr,lfn)
        failed[lfn] = errStr
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def replicateAndRegister(self,lfn,destSE,sourceSE='',destPath='',localCache=''):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    successful = {}
    failed = {}
    gLogger.verbose("ReplicaManager.replicateAndRegister: Attempting to replicate %s to %s." % (lfn,destSE))
    startReplication = time.time()
    res = self.__replicate(lfn,destSE,sourceSE,destPath)
    replicationTime = time.time()-startReplication
    if not res['OK']:
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to replicate file."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info("ReplicaManager.replicateAndRegister: %s already present at %s." % (lfn,destSE))
      successful[lfn] = {'replicate':0,'register':0}
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)
    successful[lfn] = {'replicate':replicationTime}

    destPfn = res['Value']['DestPfn']
    destSE = res['Value']['DestSE']
    gLogger.verbose("ReplicaManager.replicateAndRegister: Attempting to register %s at %s." % (destPfn,destSE))
    replicaTuple = (lfn,destPfn,destSE)
    startRegistration = time.time()
    res = self.registerReplica(replicaTuple)
    registrationTime = time.time()-startRegistration
    if not res['OK']:
      # Need to return to the client that the file was replicated but not registered
      errStr = "ReplicaManager.replicateAndRegister: Completely failed to register replica."
      gLogger.error(errStr,res['Message'])
      failed[lfn] = {'Registration':{'LFN':lfn,'TargetSE':destSE,'PFN':destPfn}}
    else:
      if res['Value']['Successful'].has_key(lfn):
        gLogger.info("ReplicaManager.replicateAndRegister: Successfully registered replica.")
        successful[lfn]['register'] = registrationTime
      else:
        errStr = "ReplicaManager.replicateAndRegister: Failed to register replica."
        gLogger.info(errStr,res['Value']['Failed'][lfn])
        failed[lfn] = {'Registration':{'LFN':lfn,'TargetSE':destSE,'PFN':destPfn}}
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def replicate(self,lfn,destSE,sourceSE='',destPath='',localCache=''):
    """ Replicate a LFN to a destination SE and register the replica.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
        'localCache' is the local file system location to be used as a temporary cache
    """
    gLogger.verbose("ReplicaManager.replicate: Attempting to replicate %s to %s." % (lfn,destSE))
    res = self.__replicate(lfn,destSE,sourceSE,destPath)
    if not res['OK']:
      errStr = "ReplicaManager.replicate: Replication failed."
      gLogger.error(errStr,"%s %s" % (lfn,destSE))
      return res
    if not res['Value']:
      # The file was already present at the destination SE
      gLogger.info("ReplicaManager.replicate: %s already present at %s." % (lfn,destSE))
      return res
    return S_OK(lfn)

  def __replicate(self,lfn,destSE,sourceSE='',destPath=''):
    """ Replicate a LFN to a destination SE.

        'lfn' is the LFN to be replicated
        'destSE' is the Storage Element the file should be replicated to
        'sourceSE' is the source for the file replication (where not specified all replicas will be attempted)
        'destPath' is the path on the destination storage element, if to be different from LHCb convention
    """
    gLogger.verbose("ReplicaManager.__replicate: Performing replication initialization.")
    res = self.__initializeReplication(lfn,sourceSE,destSE)
    if not res['OK']:
      gLogger.error("ReplicaManager.__replicate: Replication initialisation failed.",lfn)
      return res
    destStorageElement = res['Value']['DestStorage']
    lfnReplicas = res['Value']['Replicas']
    destSE = res['Value']['DestSE']
    catalogueSize = res['Value']['CatalogueSize']
    ###########################################################
    # If the LFN already exists at the destination we have nothing to do
    if lfnReplicas.has_key(destSE):
      gLogger.info("ReplicaManager.__replicate: LFN is already registered at %s." % destSE)
      return S_OK()
    ###########################################################
    # Resolve the best source storage elements for replication
    gLogger.verbose("ReplicaManager.__replicate: Determining the best source replicas.")
    res = self.__resolveBestReplicas(sourceSE,lfnReplicas,catalogueSize)
    if not res['OK']:
      gLogger.error("ReplicaManager.__replicate: Best replica resolution failed.", lfn)
      return res
    replicaPreference = res['Value']
    ###########################################################
    # Now perform the replication for the file
    if destPath:
      destPath = '%s/%s' % (destPath,os.path.basename(lfn))
    else:
      destPath = lfn
    res = destStorageElement.getPfnForLfn(destPath)
    if not res['OK']:
      errStr = "ReplicaManager.__replicate: Failed to generate destination PFN."
      gLogger.error(errStr,res['Message'])   
      return S_ERROR(errStr)
    destPfn = res['Value']
    for sourceSE,sourcePfn in replicaPreference:
      gLogger.verbose("ReplicaManager.__replicate: Attempting replication from %s to %s." % (sourceSE,destSE))
      fileDict = {destPfn:sourcePfn}
      res = destStorageElement.replicateFile(fileDict,catalogueSize,singleFile=True)
      if res['OK']:
        gLogger.info("ReplicaManager.__replicate: Replication successful.")
        resDict = {'DestSE':destSE,'DestPfn':destPfn}
        return S_OK(resDict)
      else:
        errStr = "ReplicaManager.__replicate: Replication failed."
        gLogger.error(errStr,"%s from %s to %s." % (lfn,sourceSE,destSE))
    ##########################################################
    # If the replication failed for all sources give up
    errStr = "ReplicaManager.__replicate: Failed to replicate with all sources."
    gLogger.error(errStr,lfn)
    return S_ERROR(errStr)

  def __initializeReplication(self,lfn,sourceSE,destSE,):
    ###########################################################
    # Check that the destination storage element is sane and resolve its name
    gLogger.verbose("ReplicaManager.__initializeReplication: Verifying destination Storage Element validity (%s)." % destSE)
    destStorageElement = StorageElement(destSE)
    if not destStorageElement.isValid()['Value']:
      errStr = "ReplicaManager.__initializeReplication: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,destSE)
      return S_ERROR(errStr)
    destSE = destStorageElement.getStorageElementName()['Value']
    gLogger.info("ReplicaManager.__initializeReplication: Destination Storage Element verified.")
    ###########################################################
    # Get the LFN replicas from the file catalogue
    gLogger.verbose("ReplicaManager.__initializeReplication: Attempting to obtain replicas for %s." % lfn)
    res = self.fileCatalogue.getReplicas(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: Completely failed to get replicas for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.__initializeReplication: Failed to get replicas for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
    gLogger.info("ReplicaManager.__initializeReplication: Successfully obtained replicas for LFN.")
    lfnReplicas = res['Value']['Successful'][lfn]
    ###########################################################
    # If the file catalogue size is zero fail the transfer
    gLogger.verbose("ReplicaManager.__initializeReplication: Attempting to obtain size for %s." % lfn)
    res = self.fileCatalogue.getFileSize(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.__initializeReplication: Completely failed to get size for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Message']))
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.__initializeReplication: Failed to get size for LFN."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      return S_ERROR("%s %s" % (errStr,res['Value']['Failed'][lfn]))
    catalogueSize = res['Value']['Successful'][lfn]
    if catalogueSize == 0:
      errStr = "ReplicaManager.__initializeReplication: Registered file size is 0."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)
    gLogger.info("ReplicaManager.__initializeReplication: File size determined to be %s." % catalogueSize)
    ###########################################################
    # Check whether the destination storage element is banned
    gLogger.verbose("ReplicaManager.__initializeReplication: Determining whether %s is banned." % destSE)
    configStr = '/Resources/StorageElements/BannedTarget'
    bannedTargets = gConfig.getValue(configStr,[])
    if destSE in bannedTargets:
      infoStr = "ReplicaManager.__initializeReplication: Destination Storage Element is currently banned."
      gLogger.info(infoStr,destSE)
      return S_ERROR(infoStr)
    gLogger.info("ReplicaManager.__initializeReplication: Destination site not banned.")
    ###########################################################
    # Check whether the supplied source SE is sane
    gLogger.verbose("ReplicaManager.__initializeReplication: Determining whether source Storage Element is sane.")
    configStr = '/Resources/StorageElements/BannedSource'
    bannedSources = gConfig.getValue(configStr,[])
    if sourceSE:
      if not lfnReplicas.has_key(sourceSE):
        errStr = "ReplicaManager.__initializeReplication: LFN does not exist at supplied source SE."
        gLogger.error(errStr,"%s %s" % (lfn,sourceSE))
        return S_ERROR(errStr)
      elif sourceSE in bannedSources:
        infoStr = "ReplicaManager.__initializeReplication: Supplied source Storage Element is currently banned."
        gLogger.info(infoStr,sourceSE)
        return S_ERROR(errStr)
    gLogger.info("ReplicaManager.__initializeReplication: Replication initialization successful.")
    resDict = {'DestStorage':destStorageElement,'DestSE':destSE,'Replicas':lfnReplicas,'CatalogueSize':catalogueSize}
    return S_OK(resDict)

  def __resolveBestReplicas(self,sourceSE,lfnReplicas,catalogueSize):
    ###########################################################
    # Determine the best replicas (remove banned sources, invalid storage elements and file with the wrong size)
    configStr = '/Resources/StorageElements/BannedSource'
    bannedSources = gConfig.getValue(configStr,[])
    gLogger.info("ReplicaManager.__resolveBestReplicas: Obtained current banned sources.")
    replicaPreference = []
    for diracSE,pfn in lfnReplicas.items():
      if sourceSE and diracSE != sourceSE:
        gLogger.info("ReplicaManager.__resolveBestReplicas: %s replica not requested." % diracSE)
      elif diracSE in bannedSources:
        gLogger.info("ReplicaManager.__resolveBestReplicas: %s is currently banned as a source." % diracSE)
      else:
        gLogger.info("ReplicaManager.__resolveBestReplicas: %s is available for use." % diracSE)
        storageElement = StorageElement(diracSE)
        if storageElement.isValid()['Value']:
          if storageElement.getRemoteProtocols()['Value']:
            gLogger.verbose("ReplicaManager.__resolveBestReplicas: Attempting to get source pfns for remote protocols.")
            res = storageElement.getPfnForProtocol(pfn,self.thirdPartyProtocols)
            if res['OK']:
              sourcePfn = res['Value']
              gLogger.verbose("ReplicaManager.__resolveBestReplicas: Attempting to get source file size.")
              res = storageElement.getFileSize(sourcePfn)
              if res['OK']:
                if res['Value']['Successful'].has_key(sourcePfn):
                  sourceFileSize = res['Value']['Successful'][sourcePfn]
                  gLogger.info("ReplicaManager.__resolveBestReplicas: Source file size determined to be %s." % sourceFileSize)
                  if catalogueSize == sourceFileSize:
                    fileTuple = (diracSE,sourcePfn)
                    replicaPreference.append(fileTuple)
                  else:
                    errStr = "ReplicaManager.__resolveBestReplicas: Catalogue size and physical file size mismatch."
                    gLogger.error(errStr,"%s %s" % (diracSE,sourcePfn))
                else:
                  errStr = "ReplicaManager.__resolveBestReplicas: Failed to get physical file size."
                  gLogger.error(errStr,"%s %s: %s" % (sourcePfn,diracSE,res['Value']['Failed'][sourcePfn]))
              else:
                errStr = "ReplicaManager.__resolveBestReplicas: Completely failed to get physical file size."
                gLogger.error(errStr,"%s %s: %s" % (sourcePfn,diracSE,res['Message']))
            else:
              errStr = "ReplicaManager.__resolveBestReplicas: Failed to get PFN for replication for StorageElement."
              gLogger.error(errStr,"%s %s" % (diracSE,res['Message']))
          else:
            errStr = "ReplicaManager.__resolveBestReplicas: Source Storage Element has no remote protocols."
            gLogger.info(errStr,diracSE)
        else:
          errStr = "ReplicaManager.__resolveBestReplicas: Failed to get valid Storage Element."
          gLogger.error(errStr,diracSE)
    if not replicaPreference:
      errStr = "ReplicaManager.__resolveBestReplicas: Failed to find any valid source Storage Elements."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    else:
      return S_OK(replicaPreference)

  ###################################################################
  #
  # These are the file catalog write methods
  #

  def registerFile(self,fileTuple,catalog=''):
    """ Register a file.

        'fileTuple' is the file tuple to be registered of the form (lfn,physicalFile,fileSize,storageElementName,fileGuid)
    """
    if type(fileTuple) == types.ListType:
      fileTuples = fileTuple
    elif type(fileTuple) == types.TupleType:
      fileTuples = [fileTuple]
    else:
      errStr = "ReplicaManager.registerFile: Supplied file info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.registerFile: Attempting to register %s files." % len(fileTuples))
    res = self.__registerFile(fileTuples,catalog)
    if not res['OK']:
      errStr = "ReplicaManager.registerFile: Completely failed to register files."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    return res

  def __registerFile(self,fileTuples,catalog):
    seDict = {}
    for lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum in fileTuples:
      if not seDict.has_key(storageElementName):
        seDict[storageElementName] = []
      seDict[storageElementName].append((lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum))
    successful = {}
    failed = {}
    fileTuples = []
    for storageElementName,fileTuple in seDict.items():
      destStorageElement = StorageElement(storageElementName)
      if not destStorageElement.isValid()['Value']:
        errStr = "ReplicaManager.__registerFile: Failed to instantiate destination Storage Element."
        gLogger.error(errStr,storageElementName)
        for lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum in fileTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn,physicalFile,fileSize,storageElementName,fileGuid,checksum in fileTuple:
          res = destStorageElement.getPfnForProtocol(physicalFile,self.registrationProtocol,withPort=False)
          if not res['OK']:
            pfn = physicalFile
          else:
            pfn = res['Value']
          tuple = (lfn,pfn,fileSize,storageElementName,fileGuid,checksum)
          fileTuples.append(tuple)
    gLogger.verbose("ReplicaManager.__registerFile: Resolved %s files for registration." % len(fileTuples))
    if catalog:
      fileCatalog = FileCatalog(catalog)
      res = fileCatalog.addFile(fileTuples)
    else:
      res = self.fileCatalogue.addFile(fileTuples)
    if not res['OK']:
      errStr = "ReplicaManager.__registerFile: Completely failed to register files."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def registerReplica(self,replicaTuple,catalog=''):
    """ Register a replica supplied in the replicaTuples.

        'replicaTuple' is a tuple or list of tuples of the form (lfn,pfn,se)
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(replicaTuple) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.registerReplica: Supplied file info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.registerReplica: Attempting to register %s replicas." % len(replicaTuples))
    res = self.__registerReplica(replicaTuples,catalog)
    if not res['OK']:
      errStr = "ReplicaManager.registerReplica: Completely failed to register replicas."
      gLogger.error(errStr,res['Message'])
    return res

  def __registerReplica(self,replicaTuples,catalog):
    seDict = {}
    for lfn,pfn,storageElementName in replicaTuples:
      if not seDict.has_key(storageElementName):
        seDict[storageElementName] = []
      seDict[storageElementName].append((lfn,pfn))
    successful = {}
    failed = {}
    replicaTuples = []
    for storageElementName,replicaTuple in seDict.items():
      destStorageElement = StorageElement(storageElementName)
      if not destStorageElement.isValid()['Value']:
        errStr = "ReplicaManager.__registerReplica: Failed to instantiate destination Storage Element."
        gLogger.error(errStr,storageElementName)
        for lfn,pfn in replicaTuple:
          failed[lfn] = errStr
      else:
        storageElementName = destStorageElement.getStorageElementName()['Value']
        for lfn,pfn in replicaTuple:
          res = destStorageElement.getPfnForProtocol(pfn,self.registrationProtocol,withPort=False)
          if not res['OK']:
            failed[lfn] = res['Message']
          else:
            replicaTuple = (lfn,res['Value'],storageElementName,False)
            replicaTuples.append(replicaTuple)
    gLogger.verbose("ReplicaManager.__registerReplica: Successfully resolved %s replicas for registration." % len(replicaTuples))
    if catalog:
      fileCatalog = FileCatalog(catalog)
      res = fileCatalog.addReplica(replicaTuples)
    else:
      res = self.fileCatalogue.addReplica(replicaTuples)
    if not res['OK']:
      errStr = "ReplicaManager.__registerReplica: Completely failed to register replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setReplicaProblematic(self,replicaTuple,sourceComponent=''):
    """ This method updates the status of the replica in the FileCatalog and the IntegrityDB
        The supplied replicaTuple should be of the form (lfn,pfn,se,prognosis)

        lfn - the lfn of the file
        pfn - the pfn if available (otherwise '')
        se - the storage element of the problematic replica (otherwise '')
        prognosis - this is given to the integrity DB and should reflect the problem observed with the file

        sourceComponent is the component issuing the request.
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(replicaTuple) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.setReplicaProblematic: Supplied replica info must be tuple of list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.registerReplica: Attempting to update %s replicas." % len(replicaTuples))
    statusTuples = []
    successful = {}
    failed = {}
    integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
    for lfn,pfn,se,reason in replicaTuples:
      fileMetadata = {'Prognosis':reason,'LFN':lfn,'PFN':pfn,'StorageElement':se}
      res = integrityDB.insertProblematic(sourceComponent,fileMetadata)
      if res['OK']:
        statusTuples.append((lfn,pfn,se,'Problematic'))
      else:
        failed[lfn] = res['Message']
    res = self.fileCatalog.setReplicaStatus(statusTuples)
    if not res['OK']:
      errStr = "ReplicaManager.setReplicaProblematic: Completely failed to update replicas."
      gLogger.error(errStr,res['Message'])
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  ###################################################################
  #
  # These are the removal methods for physical and catalogue removal
  #

  def removeFile(self,lfn):
    """ Remove the file (all replicas) from Storage Elements and file catalogue

        'lfn' is the file to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeFile: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.removeFile: Attempting to remove %s files from Storage and Catalogue." % len(lfns))
    gLogger.verbose("ReplicaManager.removeFile: Attempting to obtain replicas for %s lfns." % len(lfns))
    res = self.fileCatalogue.exists(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to determine existance of lfns."
      gLogger.error(errStr,res['Message'])
      return res
    successful = {}
    existingFiles = []
    for lfn,exists in res['Value']['Successful'].items():
      if not exists:
        successful[lfn] = True
      else:
        existingFiles.append(lfn)
    res = self.fileCatalogue.getReplicas(existingFiles)
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    lfnDict = res['Value']['Successful']
    res = self.__removeFile(lfnDict)
    if not res['OK']:
      errStr = "ReplicaManager.removeFile: Completely failed to remove files."
      gLogger.error(errStr,res['Message'])
      return res
    failed.update(res['Value']['Failed'])
    successful.update(res['Value']['Successful'])
    resDict = {'Successful':successful,'Failed':failed}
    gDataStoreClient.commit()
    return S_OK(resDict)

  def __removeFile(self,lfnDict):
    storageElementDict = {}
    for lfn,repDict in lfnDict.items():
      for se,pfn in repDict.items():
        if not storageElementDict.has_key(se):
          storageElementDict[se] = []
        storageElementDict[se].append((lfn,pfn))
    failed = {}
    for storageElementName,fileTuple in storageElementDict.items():
      res = self.__removeReplica(storageElementName,fileTuple)
      if not res['OK']:
        errStr = res['Message']
        for lfn,pfn in fileTuple:
          if not failed.has_key(lfn):
            failed[lfn] = ''
          failed[lfn] = "%s %s" % (failed[lfn],errStr)
      else:
        for lfn,error in res['Value']['Failed'].items():
          if not failed.has_key(lfn):
            failed[lfn] = ''
          failed[lfn] = "%s %s" % (failed[lfn],error)
    completelyRemovedFiles = []
    for lfn in lfnDict.keys():
      if not failed.has_key(lfn):
        completelyRemovedFiles.append(lfn)
    res = self.fileCatalogue.removeFile(completelyRemovedFiles)
    failed.update(res['Value']['Failed'])
    successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeReplica(self,storageElementName,lfn):
    """ Remove replica at the supplied Storage Element from Storage Element then file catalogue

       'storageElementName' is the storage where the file is to be removed
       'lfn' is the file to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeReplica: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.removeReplica: Attempting to remove catalogue entry for %s lfns at %s." % (len(lfns),storageElementName))
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removeReplica: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn,repDict in res['Value']['Successful'].items():
      if not repDict.has_key(storageElementName):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        replicaTuple = (lfn,sePfn)
        replicaTuples.append(replicaTuple)
    res = self.__removeReplica(storageElementName,replicaTuples)
    failed.update(res['Value']['Failed'])
    successful.update(res['Value']['Successful'])
    resDict = {'Successful':successful,'Failed':failed}
    gDataStoreClient.commit()
    return S_OK(resDict)

  def __removeReplica(self,storageElementName,fileTuple):
    pfnDict = {}
    for lfn,pfn in fileTuple:
      pfnDict[pfn] = lfn
    failed = {}
    res = self.__removePhysicalReplica(storageElementName,pfnDict.keys())
    if not res['OK']:
      errStr = "ReplicaManager.__removeReplica: Failed to remove catalog replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    for pfn,error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    replicaTuples = []
    for pfn in res['Value']['Successful'].keys():
      replicaTuple = (pfnDict[pfn],pfn,storageElementName)
      replicaTuples.append(replicaTuple)
    successful = {}
    res = self.__removeCatalogReplica(replicaTuples)
    if not res['OK']:
      errStr = "ReplicaManager.__removeReplica: Completely failed to remove physical files."
      gLogger.error(errStr,res['Message'])
      for lfn in pfnDict.values():
        if not failed.has_key(lfn):
          failed[lfn] = errStr
    else:
      failed.update(res['Value']['Failed'])
      successful = res['Value']['Successful']
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeCatalogReplica(self,storageElementName,lfn):
    """ Remove replica from the file catalog

       'lfn' are the file to be removed
       'storageElementName' is the storage where the file is to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removeCatalogReplica: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.removeCatalogReplica: Attempting to remove catalogue entry for %s lfns at %s." % (len(lfns),storageElementName))
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removeCatalogReplica: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = {}
    replicaTuples = []
    for lfn,repDict in res['Value']['Successful'].items():
      if not repDict.has_key(storageElementName):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        replicaTuple = (lfn,sePfn,storageElementName)
        replicaTuples.append(replicaTuple)
    gLogger.verbose("ReplicaManager.removeCatalogReplica: Resolved %s pfns for catalog removal at %s." % (len(replicaTuples), storageElementName))
    res = self.__removeCatalogReplica(replicaTuples)
    failed.update(res['Value']['Failed'])
    successful.update(res['Value']['Successful'])
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeCatalogPhysicalFileNames(self,replicaTuple):
    """ Remove replicas from the file catalog specified by replica tuple

       'replicaTuple' is a tuple containing the replica to be removed and is of the form (lfn,pfn,se)
    """
    if type(replicaTuple) == types.ListType:
      replicaTuples = replicaTuple
    elif type(lfn) == types.TupleType:
      replicaTuples = [replicaTuple]
    else:
      errStr = "ReplicaManager.removeCatalogPhysicalFileNames: Supplied info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.__removeCatalogReplica(replicaTuples)
    return res

  def __removeCatalogReplica(self,replicaTuple):
    oDataOperation = self.__initialiseAccountingObject('removeCatalogReplica','',len(replicaTuple))
    oDataOperation.setStartTime()
    start= time.time()
    res = self.fileCatalogue.removeReplica(replicaTuple)
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey('RegistrationTime',time.time()-start)
    if not res['OK']:
      oDataOperation.setValueByKey('RegistrationOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      gDataStoreClient.addRegister(oDataOperation)
      errStr = "ReplicaManager.__removeCatalogReplica: Completely failed to remove replica."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    for lfn in res['Value']['Successful'].keys():
      infoStr = "ReplicaManager.__removeCatalogReplica: Successfully removed replica."
      gLogger.info(infoStr,lfn)
    for lfn,error in res['Value']['Failed'].items():
      errStr = "ReplicaManager.__removeCatalogReplica: Failed to remove replica."
      gLogger.error(errStr,"%s %s" % (lfn,error))
    oDataOperation.setValueByKey('RegistrationOK',len(res['Value']['Successful'].keys()))
    gDataStoreClient.addRegister(oDataOperation)
    return res

  def removePhysicalReplica(self,storageElementName,lfn):
    """ Remove replica from Storage Element.

       'lfn' are the files to be removed
       'storageElementName' is the storage where the file is to be removed
    """
    if type(lfn) == types.ListType:
      lfns = lfn
    elif type(lfn) == types.StringType:
      lfns = [lfn]
    else:
      errStr = "ReplicaManager.removePhysicalReplica: Supplied lfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    gLogger.verbose("ReplicaManager.removePhysicalReplica: Attempting to remove %s lfns at %s." % (len(lfns),storageElementName))
    gLogger.verbose("ReplicaManager.removePhysicalReplica: Attempting to resolve replicas.")
    res = self.fileCatalogue.getReplicas(lfns)
    if not res['OK']:
      errStr = "ReplicaManager.removePhysicalReplica: Completely failed to get replicas for lfns."
      gLogger.error(errStr,res['Message'])
      return res
    failed = res['Value']['Failed']
    successful = {}
    pfnDict = {}
    for lfn,repDict in res['Value']['Successful'].items():
      if not lfnReplicas.has_key(storageElementName):
        # The file doesn't exist at the storage element so don't have to remove it
        successful[lfn] = True
      else:
        sePfn = repDict[storageElementName]
        pfnDict[sePfn] = lfn
    gLogger.verbose("ReplicaManager.removePhysicalReplica: Resolved %s pfns for removal at %s." % (len(pfnDict.keys()), storageElementName))
    res = self.__removePhysicalReplica(storageElementName,pfnDict.keys())
    for pfn,error in res['Value']['Failed'].items():
      failed[pfnDict[pfn]] = error
    for pfn in res['Value']['Successful'].keys():
      successful[pfnDict[pfn]]
    resDict = {'Successful':successful,'Failed':failed}
    return res

  def removePhysicalFile(self,storageElementName,pfnToRemove):
    """ This removes physical files given by a the physical file names

        'storageElementName' is the storage element where the file should be removed from
        'pfnsToRemove' is the physical files
    """
    if type(pfnToRemove) == types.ListType:
      pfns = pfnToRemove
    elif type(pfnToRemove) == types.StringType:
      pfns = [pfnToRemove]
    else:
      errStr = "ReplicaManager.removePhysicalFile: Supplied pfns must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    res = self.__removePhysicalReplica(storageElementName, pfns)
    return res

  def __removePhysicalReplica(self,storageElementName,pfnsToRemove):
    gLogger.verbose("ReplicaManager.__removePhysicalReplica: Attempting to remove %s pfns at %s." % (len(pfnsToRemove),storageElementName))
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to instantiate Storage Element for removal."
      gLogger.error(errStr,storageElement)
      return S_ERROR(errStr)
    oDataOperation = self.__initialiseAccountingObject('removePhysicalReplica',storageElementName,len(pfnsToRemove))
    oDataOperation.setStartTime()
    start= time.time()
    res = storageElement.removeFile(pfnsToRemove)
    oDataOperation.setEndTime()
    oDataOperation.setValueByKey('TransferTime',time.time()-start)
    if not res['OK']:
      oDataOperation.setValueByKey('TransferOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      gDataStoreClient.addRegister(oDataOperation)
      errStr = "ReplicaManager.__removePhysicalReplica: Failed to remove replicas."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    else:
      oDataOperation.setValueByKey('TransferOK',len(res['Value']['Successful'].keys()))
      gDataStoreClient.addRegister(oDataOperation)
      infoStr = "ReplicaManager.__removePhysicalReplica: Successfully issued removal request."
      gLogger.info(infoStr)
      return res

  #def removeReplica(self,lfn,storageElementName,singleFile=False):
  #def putReplica(self,lfn,storageElementName,singleFile=False):
  #def replicateReplica(self,lfn,size,storageElementName,singleFile=False):

  #########################################################################
  #
  # File transfer methods
  #

  def put(self,lfn,file,diracSE,path=None):
    """ Put a local file to a Storage Element

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    # Check that the local file exists
    if not os.path.exists(file):
      errStr = "ReplicaManager.put: Supplied file does not exist."
      gLogger.error(errStr, file)
      return S_ERROR(errStr)
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname(lfn)
    # Obtain the size of the local file
    size = getSize(file)
    if size == 0:
      errStr = "ReplicaManager.put: Supplied file is zero size."
      gLogger.error(errStr,file)
      return S_ERROR(errStr)
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename(lfn)
    localFileName = os.path.basename(file)
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.put: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    destinationSE = storageElement.getStorageElementName()['Value']
    res = storageElement.getPfnForLfn(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to generate destination PFN."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    destPfn = res['Value']
    fileDict = {destPfn:file}    

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    startTime = time.time()
    res = storageElement.putFile(fileDict,singleFile=True)
    putTime = time.time() - startTime
    if not res['OK']:
      errStr = "ReplicaManager.put: Failed to put file to Storage Element."
      failed[lfn] = res['Message']
      gLogger.error(errStr,"%s: %s" % (file,res['Message']))
    else:
      gLogger.info("ReplicaManager.put: Put file to storage in %s seconds." % putTime)
      successful[lfn] = putTime
    resDict = {'Successful': successful,'Failed':failed}
    return S_OK(resDict)

  def putAndRegister(self,lfn,file,diracSE,guid=None,path=None,checksum=None,catalog=None):
    """ Put a local file to a Storage Element and register in the File Catalogues

        'lfn' is the file LFN
        'file' is the full path to the local file
        'diracSE' is the Storage Element to which to put the file
        'guid' is the guid with which the file is to be registered (if not provided will be generated)
        'path' is the path on the storage where the file will be put (if not provided the LFN will be used)
    """
    res = self.__getClientCertGroup()
    # Instantiate the desired file catalog
    if catalog:
      self.fileCatalogue = FileCatalog(catalog)
    else:
      self.fileCatalogue = FileCatalog()
    # Check that the local file exists
    if not os.path.exists(file):
      errStr = "ReplicaManager.putAndRegister: Supplied file does not exist."
      gLogger.error(errStr, file)
      return S_ERROR(errStr)
    # If the path is not provided then use the LFN path
    if not path:
      path = os.path.dirname(lfn)
    # Obtain the size of the local file
    size = getSize(file)
    if size == 0:
      errStr = "ReplicaManager.putAndRegister: Supplied file is zero size."
      gLogger.error(errStr,file)
      return S_ERROR(errStr)
    # If the GUID is not given, generate it here
    if not guid:
      guid = makeGuid(file)
    if not checksum:
      checksum = fileAdler(file)
    res = self.fileCatalogue.exists({lfn:guid})
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Completey failed to determine existence of destination LFN."
      gLogger.error(errStr,lfn)
      return res
    if not res['Value']['Successful'].has_key(lfn):
      errStr = "ReplicaManager.putAndRegister: Failed to determine existence of destination LFN."
      gLogger.error(errStr,lfn)
      return S_ERROR(errStr)
    if res['Value']['Successful'][lfn]:
      if res['Value']['Successful'][lfn] == lfn:
        errStr = "ReplicaManager.putAndRegister: The supplied LFN already exists in the File Catalog."
        gLogger.error(errStr,lfn)
      else:
        errStr = "ReplicaManager.putAndRegister: This file GUID already exists for another file. Please remove it and try again."
        gLogger.error(errStr,res['Value']['Successful'][lfn])
      return S_ERROR("%s %s" % (errStr,res['Value']['Successful'][lfn]))
    # If the local file name is not the same as the LFN filename then use the LFN file name
    alternativeFile = None
    lfnFileName = os.path.basename(lfn)
    localFileName = os.path.basename(file)
    if not lfnFileName == localFileName:
      alternativeFile = lfnFileName

    ##########################################################
    #  Instantiate the destination storage element here.
    storageElement = StorageElement(diracSE)
    if not storageElement.isValid()['Value']:
      errStr = "ReplicaManager.putAndRegister: Failed to instantiate destination StorageElement."
      gLogger.error(errStr,diracSE)
      return S_ERROR(errStr)
    destinationSE = storageElement.getStorageElementName()['Value']
    res = storageElement.getPfnForLfn(lfn)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to generate destination PFN."
      gLogger.error(errStr,res['Message'])
      return S_ERROR(errStr)
    destPfn = res['Value']
    fileDict = {destPfn:file}

    successful = {}
    failed = {}
    ##########################################################
    #  Perform the put here.
    oDataOperation = self.__initialiseAccountingObject('putAndRegister',diracSE,1)
    oDataOperation.setStartTime()
    oDataOperation.setValueByKey('TransferSize',size)
    startTime = time.time()
    res = storageElement.putFile(fileDict,True)
    putTime = time.time() - startTime
    oDataOperation.setValueByKey('TransferTime',putTime)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Failed to put file to Storage Element."
      oDataOperation.setValueByKey('TransferOK',0)
      oDataOperation.setValueByKey('FinalStatus','Failed')
      oDataOperation.setEndTime()
      gDataStoreClient.addRegister(oDataOperation)
      gLogger.error(errStr,"%s: %s" % (file,res['Message']))
      return S_ERROR("%s %s" % (errStr,res['Message']))
    successful[lfn] = {'put': putTime}

    ###########################################################
    # Perform the registration here
    oDataOperation.setValueByKey('RegistrationTotal',1)
    fileTuple = (lfn,destPfn,size,destinationSE,guid,checksum)
    registerDict = {'LFN':lfn,'PFN':destPfn,'Size':size,'TargetSE':destinationSE,'GUID':guid,'Addler':checksum}
    startTime = time.time()
    res = self.registerFile(fileTuple)
    registerTime = time.time() - startTime
    oDataOperation.setValueByKey('RegistrationTime',registerTime)
    if not res['OK']:
      errStr = "ReplicaManager.putAndRegister: Completely failed to register file."
      gLogger.error(errStr,res['Message'])
      failed[lfn] = {'register':registerDict}
      oDataOperation.setValueByKey('FinalStatus','Failed')
    elif res['Value']['Failed'].has_key(lfn):
      errStr = "ReplicaManager.putAndRegister: Failed to register file."
      gLogger.error(errStr,"%s %s" % (lfn,res['Value']['Failed'][lfn]))
      oDataOperation.setValueByKey('FinalStatus','Failed')
      failed[lfn] = {'register':registerDict}
    else:
      successful[lfn]['register'] = registerTime
      oDataOperation.setValueByKey('RegistrationOK',1)
    oDataOperation.setEndTime()
    gDataStoreClient.addRegister(oDataOperation)
    resDict = {'Successful': successful,'Failed':failed}
    return S_OK(resDict)

  ##########################################################################
  #
  # These are the wrapper functions for doing simple replica->SE operations
  # (Operations requiring write access to a catalog are not performed this way)
  #

  def getReplicaIsFile(self,lfn,storageElementName,singleFile=False):
    """ Determine whether the supplied lfns are files at the supplied StorageElement

        'lfn' is the file(s) to check
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'isFile')
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'isFile')

  def getReplicaSize(self,lfn,storageElementName,singleFile=False):
    """ Obtain the file size for the lfns at the supplied StorageElement
  
        'lfn' is the file(s) for which to get the size
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'getFileSize')
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'getFileSize')

  def getReplicaAccessUrl(self,lfn,storageElementName,singleFile=False):
    """ Obtain the access url for lfns at the supplied StorageElement
        
        'lfn' is the file(s) for which to obtain access URLs
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'getAccessUrl')
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'getAccessUrl')

  def getReplicaMetadata(self,lfn,storageElementName,singleFile=False):
    """ Obtain the file metadata for lfns at the supplied StorageElement

        'lfn' is the file(s) for which to get metadata
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'getFileMetadata')
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'getFileMetadata')

  def prestageReplica(self,lfn,storageElementName,singleFile=False):
    """ Issue prestage requests for the lfns at the supplied StorageElement

        'lfn' is the file(s) for which to issue prestage requests
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'prestageFile')
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'prestageFile')

  def getPrestageReplicaStatus(self,lfn,storageElementName,singleFile=False):
    """ This functionality is not supported.
    """
    return S_ERROR("This functionality is not supported. Please use getReplicaMetadata and check the 'Cached' element.")

  def pinReplica(self,lfn,storageElementName,lifetime=60*60*24,singleFile=False):
    """ Issue a pin for the lfns at the supplied StorageElement
        
        'lfn' is the file(s) for which to issue pins
        'storageElementName' is the target Storage Element
        'lifetime' is the pin lifetime (default 1 day)
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'pinFile',{'lifetime':lifetime})
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'pinFile',{'lifetime':lifetime})

  def releaseReplica(self,lfn,storageElementName,singleFile=False):
    """ Release pins for the lfns at the supplied StorageElement
    
        'lfn' is the file(s) for which to release pins
        'storageElementName' is the target Storage Element
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'releaseFile')
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'releaseFile')
  
  def getReplica(self,lfn,storageElementName,localPath=False,singleFile=False):
    """ Get the lfns to the local disk from the supplied StorageElement

        'lfn' is the file(s) for which to release pins
        'storageElementName' is the target Storage Element
        'localPath' is the local target path (default '.')
    """
    if singleFile:
      return self.__executeSingleReplicaStorageElementOperation(storageElementName,lfn,'getFile',{'localPath':localPath})
    else:
      return self.__executeReplicaStorageElementOperation(storageElementName,lfn,'getFile',{'localPath':localPath})

  def __executeSingleReplicaStorageElementOperation(self,storageElementName,lfn,method,argsDict={}):
    res = self.__executeReplicaStorageElementOperation(storageElementName,lfn,method,argsDict)
    if type(lfn) == types.ListType:
      lfn = lfn[0]
    elif type(lfn) == types.DictType:
      lfn = lfn.keys()[0]   
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key(lfn):
      errorMessage = res['Value']['Failed'][lfn]
      return S_ERROR(errorMessage)
    else:
      return S_OK(res['Value']['Successful'][lfn])

  def __executeReplicaStorageElementOperation(self,storageElementName,lfn,method,argsDict={}):
    """ A simple wrapper that allows replica querying then perform the StorageElement operation
    """
    res = self.__executeFileCatalogFunction(lfn,'getReplicas')
    if not res['OK']:
      errStr = "ReplicaManager.__executeReplicaStorageElementOperation: Completely failed to get replicas for LFNs."
      gLogger.error(errStr,res['Message']) 
      return res
    failed = res['Value']['Failed']
    for lfn,reason in res['Value']['Failed'].items():
      gLogger.error("ReplicaManager.__executeReplicaStorageElementOperation: Failed to get replicas for file.", "%s %s" % (lfn,reason))
    lfnReplicas = res['Value']['Successful']
    pfnDict = {}
    for lfn,replicas in lfnReplicas.items():
      if replicas.has_key(storageElementName):
        pfnDict = {replicas[storageElementName]:lfn}
      else:
        errStr = "ReplicaManager.__executeReplicaStorageElementOperation: File does not have replica at supplied Storage Element."
        gLogger.error(errStr, "%s %s" % (lfn,storageElementName))
        failed[lfn] = errStr
    res = self.__executeStorageElementFunction(storageElementName,pfnDict.keys(),method,argsDict)
    if not res['OK']:
      gLogger.error("ReplicaManager.__executeReplicaStorageElementOperation: Failed to execute %s StorageElement operation." % method,res['Message'])
      return res
    else:
      successful = {}
      for pfn,pfnRes in res['Value']['Successful'].items():
        successful[pfnDict[pfn]] = pfnRes
      for pfn, errorMessage in res['Value']['Failed'].items():
        failed[pfnDict[pfn]] = errorMessage
      resDict = {'Successful':successful,'Failed':failed}
      return S_OK(resDict)

  ##########################################################################
  #
  # These are the file catalog wrapper functions (read only)
  #

  def getCatalogReplicas(self,lfn,singleFile=False):
    """ Get the replicas registered for files in the FileCatalog

        'lfn' is the files to check (can be a single lfn or list of lfns)
    """
    if singleFile:
      return self.__executeSingleFileCatalogFunction(lfn,'getReplicas')
    else:
      return self.__executeFileCatalogFunction(lfn,'getReplicas')
  
  def getCatalogFileSize(self,lfn,singleFile=False):
    """ Get the size registered for files in the FileCatalog

        'lfn' is the files to check (can be a single lfn or list of lfns)
    """
    if singleFile:
      return self.__executeSingleFileCatalogFunction(lfn,'getFileSize')     
    else:
      return self.__executeFileCatalogFunction(lfn,'getFileSize')

  def __executeSingleFileCatalogFunction(self,lfn,method,argsDict={}):
    res = self.__executeFileCatalogFunction(lfn,method,argsDict)
    if type(lfn) == types.ListType:
      lfn = lfn[0]
    elif type(lfn) == types.DictType:   
      lfn = lfn.keys()[0]
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key(lfn):
      errorMessage = res['Value']['Failed'][lfn]
      return S_ERROR(errorMessage)
    else:
      return S_OK(res['Value']['Successful'][lfn])
  
  def __executeFileCatalogFunction(self,lfn,method,argsDict={}):
    """ A simple wrapper around the file catalog functionality
    """
    # First check the supplied lfn(s) are the correct format.
    if type(lfn) in types.StringTypes:
      lfns = {lfn:False}
    elif type(lfn) == types.ListType:
      lfns = {}
      for lfn in lfn:
        lfns[lfn] = False
    elif type(lfn) == types.DictType:
      lfns = lfn.copy()
    else:
      errStr = "ReplicaManager.__executeFileCatalogFunction: Supplied lfns must be string or list of strings or a dictionary." 
      gLogger.error(errStr)
      return S_ERROR(errStr)
    # Check we have some lfns
    if not lfns:
      errMessage = "ReplicaManager.__executeFileCatalogFunction: No lfns supplied."
      gLogger.error(errMessage)
      return S_ERROR(errMessage)
    gLogger.debug("ReplicaManager.__executeFileCatalogFunction: Attempting to perform '%s' operation with %s lfns." % (method,len(lfns)))
    # Check we can instantiate the file catalog correctly
    fileCatalog = FileCatalog()
    # Generate the execution string 
    lfns = lfns.keys()
    if argsDict:
      execString = "res = fileCatalog.%s(lfns" % method
      for argument,value in argsDict.items():
        if type(value) == types.StringType:  
          execString = "%s, %s='%s'" % (execString,argument,value)
        else:
          execString = "%s, %s=%s" % (execString,argument,value)
      execString = "%s)" % execString
    else:
      execString = "res = fileCatalog.%s(lfns)" % method
    # Execute the execute string
    try:
      exec(execString)
    except AttributeError,errMessage:
      exceptStr = "ReplicaManager.__executeFileCatalogFunction: Exception while perfoming %s." % method
      gLogger.exception(exceptStr,str(errMessage))
      return S_ERROR(exceptStr)
    # Return the output
    if not res['OK']:
      errStr = "ReplicaManager.__executeFileCatalogFunction: Completely failed to perform %s." % method
      gLogger.error(errStr,res['Message'])
    return res

  ##########################################################################
  #
  # These are the storage element wrapper functions
  #
  
  def getPhysicalFileExists(self,physicalFile,storageElementName,singleFile=False):
    """ Determine the existance of the physical files
        
        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'exists')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'exists')

  def getPhysicalFileIsFile(self,physicalFile,storageElementName,singleFile=False):
    """ Determine the physical paths are files

        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'isFile')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'isFile')  

  def getPhysicalFileSize(self,physicalFile,storageElementName,singleFile=False):
    """ Obtain the size of the physical files
   
        'physicalFile' is the pfn(s) size to be obtained
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'getFileSize')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'getFileSize')

  def getPhysicalFileAccessUrl(self,physicalFile,storageElementName,singleFile=False):
    """ Obtain the access url for a physical file

        'physicalFile' is the pfn(s) to access
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'getAccessUrl')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'getAccessUrl')

  def getPhysicalFileMetadata(self,physicalFile,storageElementName,singleFile=False):
    """ Obtain the metadata for physical files
      
        'physicalFile' is the pfn(s) to be checked
        'storageElementName' is the Storage Element to check
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'getFileMetadata')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'getFileMetadata')

  def removePhysicalFile(self,physicalFile,storageElementName,singleFile=False):
    """ Remove physical files
   
       'physicalFile' is the pfn(s) to be removed
       'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'removeFile')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'removeFile')

  def prestagePhysicalFile(self,physicalFile,storageElementName,singleFile=False):
    """ Prestage physical files 
  
        'physicalFile' is the pfn(s) to be pre-staged
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'prestageFile')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'prestageFile')

  def getPrestagePhysicalFileStatus(self,physicalFile,storageElementName,singleFile=False):
    """ Obtain the status of a pre-stage request
          
        'physicalFile' is the pfn(s) to obtain the status
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'prestageFileStatus')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'prestageFileStatus')

  def pinPhysicalFile(self,physicalFile,storageElementName,lifetime=60*60*24,singleFile=False):
    """ Pin physical files with a given lifetime
    
        'physicalFile' is the pfn(s) to pin
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'pinFile')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'pinFile')

  def releasePhysicalFile(self,physicalFile,storageElementName,singleFile=False):
    """ Release the pin on physical files
      
        'physicalFile' is the pfn(s) to release
        'storageElementName' is the Storage Element
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'releaseFile')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'releaseFile')

  def getPhysicalFile(self,physicalFile,storageElementName,localPath=False,singleFile=False):
    """ Get a local copy of a physical file
  
        'physicalFile' is the pfn(s) to get
        'storageElementName' is the Storage Element
    """    
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'getFile',argsDict={'localPath':localPath})
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'getFile',argsDict={'localPath':localPath})

  def putPhysicalFile(self,physicalFile,storageElementName,singleFile=False):
    """ Put a local file to the storage element

        'physicalFile' is the pfn(s) dict to put
        'storageElementName' is the StorageElement
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'putFile')
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'putFile')

  def replicatePhysicalFile(self,physicalFile,size,storageElementName,singleFile=False):
    """ Replicate a physical file to a storage element

        'physicalFile' is the pfn(s) dict to replicate
        'storageElementName' is the target StorageElement
    """
    if singleFile:
      return self.__executeSingleStorageElementFunction(storageElementName,physicalFile,'replicateFile',argsDict={sourceSize:size})
    else:
      return self.__executeStorageElementFunction(storageElementName,physicalFile,'replicateFile',argsDict={sourceSize:size})

  def __executeSingleStorageElementFunction(self,storageElementName,pfn,method,argsDict={}):
    res = self.__executeStorageElementFunction(storageElementName,pfn,method,argsDict)
    if type(pfn) == types.ListType:
      pfn = pfn[0]
    elif type(pfn) == types.DictType:   
      pfn = pfn.keys()[0]
    if not res['OK']:
      return res
    elif res['Value']['Failed'].has_key(pfn):
      errorMessage = res['Value']['Failed'][pfn]
      return S_ERROR(errorMessage)
    else:
      return S_OK(res['Value']['Successful'][pfn])
  
  def __executeStorageElementFunction(self,storageElementName,pfn,method,argsDict={}):
    """ A simple wrapper around the storage element functionality
    """
    # First check the supplied pfn(s) are the correct format.
    if type(pfn) in types.StringTypes:
      pfns = {pfn:False}
    elif type(pfn) == types.ListType:
      pfns = {}
      for url in pfn:
        pfns[url] = False
    elif type(pfn) == types.DictType:
      pfns = pfn.copy()
    else:
      errStr = "ReplicaManager.__executeStorageElementFunction: Supplied pfns must be string or list of strings or a dictionary." 
      gLogger.error(errStr)
      return S_ERROR(errStr)
    # Check we have some pfns
    if not pfns:
      errMessage = "ReplicaManager.__executeStorageElementFunction: No pfns supplied."
      gLogger.error(errMessage)
      return S_ERROR(errMessage)
    gLogger.debug("ReplicaManager.__executeStorageElementFunction: Attempting to perform '%s' operation with %s pfns." % (method,len(pfns)))
    # Check we can instantiate the storage element correctly
    storageElement = StorageElement(storageElementName)
    if not storageElement.isValid()['Value']:   
      errStr = "ReplicaManager.__executeStorageElementFunction: Failed to instantiate Storage Element"
      gLogger.error(errStr, "for performing %s at %s." % (method,storageElementName))
      return S_ERROR(errStr)
    # Generate the execution string 
    if argsDict:
      execString = "res = storageElement.%s(pfns" % method
      for argument,value in argsDict.items():
        if type(value) == types.StringType:  
          execString = "%s, %s='%s'" % (execString,argument,value)
        else:
          execString = "%s, %s=%s" % (execString,argument,value)
      execString = "%s)" % execString
    else:
      execString = "res = storageElement.%s(pfns)" % method
    # Execute the execute string
    try:
      exec(execString)
    except AttributeError,errMessage:
      exceptStr = "ReplicaManager.__executeStorageElementFunction: Exception while perfoming %s." % method
      gLogger.exception(exceptStr,str(errMessage))
      return S_ERROR(exceptStr)
    # Return the output
    if not res['OK']:
      errStr = "ReplicaManager.__executeStorageElementFunction: Completely failed to perform %s." % method
      gLogger.error(errStr,'%s : %s' % (storageElementName,res['Message']))
    return res

  def __initialiseAccountingObject(self,operation,se,files):
    accountingDict = {}
    accountingDict['OperationType'] = operation
    accountingDict['User'] = 'acsmith'
    accountingDict['Protocol'] = 'ReplicaManager'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    accountingDict['Destination'] = se
    accountingDict['TransferTotal'] = files
    accountingDict['TransferOK'] = files
    accountingDict['TransferSize'] = files
    accountingDict['TransferTime'] = 0.0
    accountingDict['FinalStatus'] = 'Successful'
    accountingDict['Source'] = gConfig.getValue('/LocalSite/Site','Unknown')
    oDataOperation = DataOperation()
    oDataOperation.setValuesFromDict(accountingDict)
    return oDataOperation

  ##########################################
  #
  # Defunct methods only there before checking backward compatability
  # 

  def getPfn(self,physicalFile,diracSE):
    """ Get a local copy of the PFN from the given Storage Element.

        'pfn' is the pfn
        'storageElement' is the DIRAC storage element
    """
    return self.getPhysicalFile(physicalFile,storageElementName)

  def  onlineRetransfer(self,storageElementName,physicalFile):
    """ Requests the online system to re-transfer files

        'storageElementName' is the storage element where the file should be removed from
        'physicalFile' is the physical files
    """
    return self.__executeStorageElementFunction(storageElementName,physicalFile,'retransferOnlineFile')

  def getReplicas(self,lfn):
    return self.getCatalogReplicas(lfn)

  def getFileSize(self,lfn):
    return self.getCatalogFileSize(lfn)
    

