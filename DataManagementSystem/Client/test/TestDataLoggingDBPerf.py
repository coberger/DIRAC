'''
Created on Jun 11, 2015

@author: Corentin Berger
'''

import time

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName


dictLong = {'files': 'first', 'targetSE': 'longTargetSE',
 'blob': 'physicalFile = blablablablablabla ,fileSize = 6536589', 'srcSE': 'longSrcSE'}

def makeSequenceA():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []
  for x in range( 5000 ):
    calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName2' + str( x ) )} ) )

  for x in range( 5000 ):
    sequence.popMethodCall()

  for call in calls :
    call.addAction( DLAction( DLFile( dictLong['files'] ), DLStatus( 'Successful' ) ,
              DLStorageElement( dictLong['srcSE'] ), DLStorageElement( dictLong['targetSE'] ), dictLong['blob'], 'errorMessage' ) )
  return sequence


def makeSequenceB():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []
  for x in range( 5 ):
    calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName2' + str( x ) )} ) )

  for x in range( 5 ):
    sequence.popMethodCall()

  for call in calls :
    for x in range( 2000 ):
      call.addAction( DLAction( DLFile( dictLong['files'] ), DLStatus( 'Successful' ) ,
              DLStorageElement( dictLong['srcSE'] + str( x ) ), DLStorageElement( dictLong['targetSE'] + str( x ) ),
              dictLong['blob'], 'errorMessage' ) )
  return sequence

seqs = []
client = DataLoggingClient()

#===============================================================================
# for x in range( 10 ) :
#   seqs.append( makeSequenceA() )
#   client.insertSequence( seqs[x] )
#===============================================================================

begin = time.time()
seqs = []
for x in range( 1 ) :
  seqs.append( makeSequenceB() )
  print ' begin insertion'
  begin = time.time()
  client.insertSequence( seqs[x] )
  t = time.time() - begin
  print 'end of insertion, time : %s' % time.strftime( '%M : %S', time.localtime( t ) )


#===============================================================================
# begin = time.time()
# res = dlc.getSequenceOnFile( 'first' )
# t = time.time() - begin
# print 'end of selection, time : %s' % time.strftime( '%M : %S', time.localtime( t ) )
#===============================================================================
