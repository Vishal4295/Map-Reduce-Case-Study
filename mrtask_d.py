from mrjob.job import MRJob
from mrjob.step import MRStep


from datetime import datetime
from datetime import timedelta

# We extend the MRJob class
# This includes our definition of map and reduce functions
class MRAvgTripTime(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        if len(row) >= 19:
            if row[0] != 'VendorID':        
                puloc = row[7]              
                putime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S")  
                dotime = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S")  
                
                duration =(dotime - putime).total_seconds()      
            
                yield puloc,duration      

                   
    def reducer(self, puloc, duration):       
        time = 0
        count = 0
        total = 0
        avg_duration = 0
        result = ''

        for time in duration:
            count = count + 1
            total = total + time
      
        avg_duration = int(total/count)
        
        hours = avg_duration // 3600
        minutes = (avg_duration % 3600) // 60
        seconds = avg_duration % 60

        result = str(hours) + 'hours ' + str(minutes) + 'minutes ' + str(seconds) + 'seconds'
        
        yield puloc,result      

if __name__ == '__main__':
    MRAvgTripTime.run()
