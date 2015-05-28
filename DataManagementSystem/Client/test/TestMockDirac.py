'''
Created on May 26, 2015

@author: Corentin Berger
'''

from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientA, ClientB, ClientC
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient


#===============================================================================
# client = DataLoggingClient()
# client.getSequenceOnFile( 'lfn3' )
#===============================================================================




c1 = ClientA( ['A', 'B', 'C', 'D'] )
c2 = ClientA( ['C', 'D'] )
c3 = ClientA( ['A', 'B'] )
c4 = ClientA( ['C', 'D'] )
c5 = ClientA( ['A', 'B'] )
c6 = ClientA( ['C', 'D'] )
c7 = ClientA( ['A', 'B'] )
c8 = ClientA( ['C', 'D'] )

#===============================================================================
# c1 = ClientB()
# c2 = ClientB()
# c3 = ClientB()
# c4 = ClientB()
#===============================================================================

#===============================================================================
# c1 = ClientC()
# c2 = ClientC()
# c3 = ClientC()
# c4 = ClientC()
# c5 = ClientC()
# c6 = ClientC()
# c7 = ClientC()
# c8 = ClientC()
#===============================================================================

#===============================================================================
# c9 = ClientC()
# c10 = ClientC()
# c11 = ClientC()
# c12 = ClientC()
# c13 = ClientC()
# c14 = ClientC()
# c15 = ClientC()
# c16 = ClientC()
#===============================================================================

c1.start()
c2.start()
c3.start()
c4.start()
c5.start()
c6.start()
c7.start()
c8.start()

#===============================================================================
# c9.start()
# c10.start()
# c11.start()
# c12.start()
# c13.start()
# c14.start()
# c15.start()
# c16.start()
#===============================================================================

c1.join()
c2.join()
c3.join()
c4.join()
c5.join()
c6.join()
c7.join()
c8.join()
c8.join()


#===============================================================================
# c9.join()
# c10.join()
# c11.join()
# c12.join()
# c13.join()
# c14.join()
# c15.join()
# c16.join()
#===============================================================================