from DIRAC import S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.TestClient     import TestClient
import random


def splitIntoSuccFailed(lfns):
  """ Randomly return some as successful, others as failed """

  successful = dict.fromkeys(random.sample(lfns, random.randint(0, len(lfns))), {})
  failed = dict.fromkeys(set(lfns) - set(successful), {})

  return successful, failed

  """ Decorator of replicateAndRegister function"""
def decorator_replicateAndRegister( func ):

  """ Create client, insert operation into data base"""
  def wrapper( lfns, srcSE, dstSE, timeout, protocol = 'srm' ):
      print "before func"
      func( lfns, srcSE, dstSE, timeout, protocol )
      print "after func"
      test_client = TestClient()
      test_client.insertOpBD()


  return wrapper

class TestFileCatalog:

  def addFile(self, lfns, seName):
    """Adding new file, registering them into seName"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

  def addReplica(self, lfns, seName):
    """Adding new replica, registering them into seName"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

  def getFileSize(self, lfns):
    """Getting file size"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

class TestStorageElement:

  def __init__(self, seName):
    self.seName  = seName

  def putFile(self, lfns, src):
    """Physicaly copying one file from src"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})

  def getFileSize(self, lfns):
    """Getting file size"""

    s, f = splitIntoSuccFailed(lfns)

    return S_OK({'Successful' : s, 'Failed' : f})


class TestDataManager:

  @decorator_replicateAndRegister
  def replicateAndRegister(self, lfns, srcSE, dstSE, timeout, protocol = 'srm'):
    """ replicate a file from one se to the other and register the new replicas"""
    print "replicateAndRegister"
    fc = TestFileCatalog()
    se = TestStorageElement( dstSE )

    res = se.putFile( lfns, srcSE )
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']

    for lfn in failed:
      failed.setdefault( lfn, {} )['Replicate'] = 'blablaMsg'

    res = fc.addReplica( successful, dstSE )

    failed.update( res['Value']['Failed'] )

    for lfn in res['Value']['Failed']:
      failed.setdefault( lfn, {} )['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'Replicate' : 1, 'Register' : 2}



    return S_OK( {'Successful' : successful, 'Failed' : failed} )


  def putAndRegister(self, lfns, localPath, dstSE):
    """ Take a local file and copy it to the dest storageElement and register the new file"""

    fc = TestFileCatalog()
    se = TestStorageElement(dstSE)

    res = se.putFile(lfns, localPath)
    failed = res['Value']['Failed']
    successful = res['Value']['Successful']

    for lfn in failed:
      failed.setdefault(lfn,{})['put'] = 'blablaMsg'

    res = fc.addFile(successful, dstSE)

    failed.update(res['Value']['Failed'])

    for lfn in res['Value']['Failed']:
      failed.setdefault(lfn,{})['Register'] = 'blablaMsg'

    successful = {}
    for lfn in res['Value']['Successful']:
      successful[lfn] = { 'put' : 1, 'Register' : 2}



    return S_OK( {'Successful' : successful, 'Failed' : failed} )

class ClientA:

  def doSomething(self):
    dm = TestDataManager()
    res = dm.replicateAndRegister([1,2,3,4,5,6,7], 'sourceSE', 'destSE', 1)
#===============================================================================
#     s = res['Value']['Successful']
#     f = res['Value']['Failed']
#     print "s : %s"%s
#     print "f : %s"%f
#
#     res = TestStorageElement('sourceSE').getFileSize(s)
#     print res
#===============================================================================

class ClientB:

  def doSomethingElse(self):
    dm = TestDataManager()
    res = dm.putAndRegister([18], '/local/path/', 'destSE')
    s = res['Value']['Successful']
    f = res['Value']['Failed']
    print "s : %s"%s
    print "f : %s"%f

    res = TestFileCatalog().getFileSize(s)
    print res

ca = ClientA()
ca.doSomething()

#===============================================================================
# cb = ClientB()
# cb.doSomethingElse()
#===============================================================================
