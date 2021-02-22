import numpy as np


A = np.random.rand(1000,50)

B = np.random.rand(50,2000)

np.savetxt('A.txt',A)
np.savetxt('B.txt',B)

C = np.dot(A,B)
np.savetxt('C.txt',C)


