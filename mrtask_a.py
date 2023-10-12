from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRVendorMaxTrips(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]    

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                vendor = row[0]             
                amount = float(row[16])     

                yield vendor, amount        

                 
    def reducer1(self, vendor, amount):     
        amt = 0
        count = 0
        total = 0
        
        for amt in amount:
            count = count + 1
            total = total + amt
        
        yield None,(count,(vendor,total))     
    
    
    def reducer2(self, _, result):             
        
        output = max(result)
        
        yield output[1]                          
        
if __name__ == '__main__':
    MRVendorMaxTrips.run()        
            
           