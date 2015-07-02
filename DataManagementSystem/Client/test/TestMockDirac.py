'''
Created on Jun 11, 2015

@author: Corentin Berger
'''


from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientA, ClientB, ClientC



client = ClientC()
client.start()
client.join()

#===============================================================================
# clientB = ClientB()
# clientB.start()
# clientB.join()
#===============================================================================
