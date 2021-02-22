from mrjob.job import MRJob
from mrjob.step import MRStep

m_rows = 0
m_columns = 0
n_rows = 0
n_columns = 0

# Matrix A - m_rows x m_cols and Matrix B - (m_cols == n_rows) x n_cols since its a scaler product


class MRScaler_Product(MRJob):

    def mapper_part1(self,file,_):
        global m_rows,m_columns,n_rows,n_columns
       
        #reading each matrix file
        x = open(file,"r")
        for line in x:
            row_lines = line.split()  #Splitting the line to get all values of the matrix row
            y= len(row_lines) # no of rows present in the matrix 
        
        while m_columns == 0:
            m_columns = y-1
            m_rows =m_rows+1
                 
        
        while n_columns == 0:
            n_columns = y - 1
            n_rows = n_rows+1
                     
        for k in range(y):    #reproducing values of k as many times as the rows in the matrix to produce all values that need to be mulplied
            yield k, ("TEMP_1", m_rows,row_lines[k])
            yield n_rows, ("TEMP_2", k, row_lines[k])

    
    def reducer_part1(self, key, values):
        M_listA =[]
        N_listB = []
        for val in values:
            if val[0] == "TEMP_1":
                M_listA.append(val)
            elif val[0] == "TEMP_2":
                N_listB.append(val)
        for m in M_listA:
            for n in N_listB:
                yield (m[1], n[1]),float(m[1]) * float(n[2])  #doing the matrix dot multiplication for matrix A and B

      
    
    def mapper_part2(self, key, values):
        yield key, values      #mutiplied values are added

    def reducer_part2(self, key, values):
       
        yield (key, sum(values))
    
    
    def steps(self):
        return [MRStep(mapper_raw=self.mapper_part1,      
                       reducer=self.reducer_part1),        
                MRStep(mapper=self.mapper_part2,          
                       reducer=self.reducer_part2)]     



if __name__ == '__main__':
    MRScaler_Product.run()
    