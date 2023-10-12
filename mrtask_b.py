from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRMaxRevenueLoc(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer1),
            MRStep(reducer=self.reducer2) ]

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                puloc  = row[7]             
                amount = float(row[16])     

                yield puloc, amount         

                
    def reducer1(self, puloc, amount):     
        
        yield None, (sum(amount),puloc)     

    
    def reducer2(self, _, amnt_puloc_pair):         
        result = max(amnt_puloc_pair)
        
        yield result[1], result[0]              


if __name__ == '__main__':
    MRMaxRevenueLoc.run()
        
        