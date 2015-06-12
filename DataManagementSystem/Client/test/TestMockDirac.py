'''
Created on Jun 11, 2015

@author: Corentin Berger
'''


from DIRAC.DataManagementSystem.Client.test.mockDirac import ClientA



client = ClientA( ['coco1'] )

client.start()
client.join()
