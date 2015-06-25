'''
Created on Jun 11, 2015

@author: Corentin Berger
'''

import time
import random
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
from DIRAC.DataManagementSystem.Client.DLSequence import DLSequence
from DIRAC.DataManagementSystem.Client.DLAction import DLAction
from DIRAC.DataManagementSystem.Client.DLFile import DLFile
from DIRAC.DataManagementSystem.Client.DLStatus import DLStatus
from DIRAC.DataManagementSystem.Client.DLStorageElement import DLStorageElement
from DIRAC.DataManagementSystem.Client.DLMethodName import DLMethodName


dictLong = {'files': 'second', 'targetSE': 'longTargetSE',
 'blob': 'physicalFile = blablablablablabla ,fileSize = 6536589', 'srcSE': 'longSrcSE'}

randomMax = 100

def makeSequenceA():
  sequence = DLSequence()
  sequence.setCaller( 'longCallerName' )
  calls = []
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()
  calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName' + str( random.randint( 0, randomMax ) ) )} ) )
  sequence.popMethodCall()
  sequence.popMethodCall()


  for call in calls :
    for x in range( 4 ):
      call.addAction( DLAction( DLFile( dictLong['files'] + str( random.randint( 0, randomMax ) ) + '.data' ) , DLStatus( 'Successful' ) ,
              DLStorageElement( dictLong['srcSE'] + str( random.randint( 0, randomMax ) ) ),
               DLStorageElement( dictLong['targetSE'] + str( random.randint( 0, randomMax ) ) ),
              dictLong['blob'], 'errorMessage' ) )
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
    calls.append( sequence.appendMethodCall( {'name': DLMethodName( 'longMethodName2' + str( x ) )} ) )

  for x in range( 10 ):
    sequence.popMethodCall()

  for call in calls :
    for x in range( 5 ):
      call.addAction( DLAction( DLFile( dictLong['files'] ), DLStatus( 'Successful' ) ,
              DLStorageElement( dictLong['srcSE'] + str( x % 20 ) ), DLStorageElement( dictLong['targetSE'] + str( x % 20 ) ),
              dictLong['blob'], 'errorMessage' ) )
  return sequence


client = DataLoggingClient()


print time.strftime( '%H :%M : %S', time.localtime( time.time() ) )

begin = time.time()
for x in range( 10000 ) :
  seq = makeSequenceA()
  begin = time.time()
  res = client.insertSequence( seq )
  if not res['OK']:
    print 'res %s' % res['Message']
  t = time.time() - begin
  if ( x % 100 ) == 0 :
    print 'end of insertion number %s , time : %s' % ( x, t )

print time.strftime( '%H : %M : %S', time.localtime( time.time() ) )

#===============================================================================
# begin = time.time()
# res = dlc.getSequenceOnFile( 'first' )
# t = time.time() - begin
# print 'end of selection, time : %s' % time.strftime( '%M : %S', time.localtime( t ) )
#===============================================================================
