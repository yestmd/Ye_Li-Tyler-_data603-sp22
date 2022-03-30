from mrjob.job import MRJob
import string

class MRRATECount(MRJob):
    
    def mapper(self, _, line):
        (business_id, date, review_id, stars,other1)=line.split(',', 4)
        (other2, cool, useful, funny)=line.rsplit(',', 3)
        
        if cool!="0" and stars=="1":
            yield "st", 1
        elif cool!="0" and stars=="2":
            yield "st", 2
        elif cool!="0" and stars=="3":
            yield "st", 3
        elif cool!="0" and stars=="4":
            yield "st", 4
        elif cool!="0" and stars=="5":
            yield "st", 5
        
        
    def reducer(self, key, values):
        
        a=0
        b=0
        for p in values:          
            a+=p
            b+=1
            
        yield "average_rate", a/b
    
if __name__ == '__main__':
    MRAveragewordCount.run()
