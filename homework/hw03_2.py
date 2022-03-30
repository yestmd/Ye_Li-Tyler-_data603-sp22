from mrjob.job import MRJob

class MRAveragewordCount(MRJob):
    
    def mapper(self, _, line):
        
        (business_id, date, review_id, stars,other1)=line.split(',', 4)
        
        if business_id !="business_id":
    
            date=date[0:7]
        
            yield date, 1

    def reducer(self, key, values):         
        yield key, sum(values)

if __name__ == '__main__':
    MRAveragewordCount.run()
