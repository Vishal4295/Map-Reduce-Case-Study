from mrjob.job import MRJob
from mrjob.step import MRStep

#importing datetime library to handle timestamp data
from datetime import datetime

class MRAvgRevenueByHour(MRJob):
    
    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID': 
                
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")            
                hour = putime.hour             
                hour_day = 'Hour ' + str(hour)  
                amount = float(row[16])        

                
                yield hour_day, amount         
                
                
    def reducer(self, hour_day, amount):       
        
        amt = 0
        count = 0
        total = 0
        avg_revenue = 0

        for amt in amount:
            count = count + 1
            total = total + amt
                 
        avg_revenue = total/count
        
       
        yield hour_day, avg_revenue      

if __name__ == '__main__':
    MRAvgRevenueByHour.run()                
                
                