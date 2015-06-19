'''
Created on Jun 11, 2015

@author: Corentin Berger
'''

import random, time
from threading import Thread

from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName


randomMax = 20

dictLong = {'files': '/lhcb/data/file', 'targetSE': '/SE/Target/se',
 'blob': 'physicalFile = blablablablablabla ,fileSize = 6536589', 'srcSE': '/SE/SRC/src'}

def makeSequenceA():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []
  for x in range( 2 ):
    calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()

  for x in range( 2 ):
    sequence.popMethodCall()

  for call in calls :
    for x in range( 2 ):
      call.addAction( DLAction( DLFile( dictLong['files'] + str( random.randint( 0, randomMax ) ) + '.data' ) , DLStatus( 'Failed' ) ,
              DLStorageElement( dictLong['srcSE'] + str( random.randint( 0, randomMax ) ) ),
               DLStorageElement( dictLong['targetSE'] + str( random.randint( 0, randomMax ) ) ),
              dictLong['blob'], 'errorMessage' ) )
  return sequence


def makeSequenceB():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []
  for x in range( 10 ):
    calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName2' + str( random.randint( 0, randomMax ) ) )} ) )

  for x in range( 10 ):
    sequence.popMethodCall()

  for call in calls :
    for x in range( 100 ):
      call.addAction( DLAction( DLFile( dictLong['files'] + str( random.randint( 0, randomMax ) ) + '.data' ) , DLStatus( 'Successful' ) ,
              DLStorageElement( dictLong['srcSE'] + str( random.randint( 0, randomMax ) ) ),
               DLStorageElement( dictLong['targetSE'] + str( random.randint( 0, randomMax ) ) ),
              dictLong['blob'], 'errorMessage' ) )
  return sequence



class SequenceA( Thread ):
  def run( self ):
    client = DataLoggingClient()
    seq = makeSequenceA()
    res = client.insertCompressedSequence( seq )
    if not res['OK']:
      print 'res %s' % res['Message']

class SequenceB( Thread ):
  def run( self ):
    client = DataLoggingClient()
    seq = makeSequenceB()
    res = client.insertCompressedSequence( seq )
    if not res['OK']:
      print 'res %s' % res['Message']

begin = time.time()
insertions = []
for x in range( 10 ) :
  #=============================================================================
  # if (x%2) == 0:
  #   insertions.append( SequenceA() )
  #   insertions[x].start()
  # else :
  #=============================================================================
    insertions.append( SequenceB() )
    insertions[x].start()

for x in range( 1 ) :
  insertions[x].join()


print 'end, time : %s' % time.strftime( '%M : %S', time.localtime( time.time() - begin ) )
