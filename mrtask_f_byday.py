from mrjob.job import MRJob
from mrjob.step import MRStep

#importing datetime library to handle timestamp data
from datetime import datetime
# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRAvgRevenueByDay(MRJob):

    def mapper(self, _, line):
    
        days = {1:'Monday',2:'Tuesday',3:'Wednesday',4:'Thursday',5:'Friday',6:'Saturday',7:'Sunday'}
        
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':
                
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  
                              
                weekday = putime.isoweekday()  
                
                dayofweek = days.get(weekday)   

                amount = float(row[16])         
                
                yield dayofweek, amount
            
    def reducer(self, day, amount):       
        
        amt = 0
        count = 0
        total = 0
        avg_revenue = 0

        for amt in amount:
            count = count + 1
            total = total + amt
  
        avg_revenue = total/count
        
        
        yield day, avg_revenue      

if __name__ == '__main__':
    MRAvgRevenueByDay.run()