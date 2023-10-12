from mrjob.job import MRJob
from mrjob.step import MRStep

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRTotalAmtTipRatio(MRJob):

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
                tip    = float(row[13])     
                
                yield puloc, (amount,tip)

    def reducer1(self, puloc, amnt_pair):
        amt = 0
        total_amnt = 0
        total_tip = 0

        for amt in amnt_pair:
            total_amnt = total_amnt + amt[0]
            total_tip  = total_tip + amt[1]
        
        if total_amnt == 0:
            avg_ratio = 0
        else:
            avg_ratio = total_tip/total_amnt
             
        yield None, (avg_ratio,puloc)
    
    def reducer2(self, _, result_pair):  
        sorted_result = sorted(result_pair, reverse = True)
        for pair in sorted_result:
            yield pair[1], pair[0]

if __name__ == '__main__':
    MRTotalAmtTipRatio.run()            