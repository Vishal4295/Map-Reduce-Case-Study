
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCountOfPaymentTypes(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,                   
                  reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]
           
    def mapper(self, _, line):                      
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                ptype  = row[9]             
                count  = 1                   
            
                yield ptype,count                           
        
    
    def reducer1(self, ptype, count):       
        
         yield None, (sum(count),ptype)
            

    def reducer2(self, _, result_pair):  
       
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]            
                                   
            
if __name__ == '__main__':
    MRCountOfPaymentTypes.run()                
