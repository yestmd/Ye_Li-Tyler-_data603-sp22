from mrjob.job import MRJob
import string

class MRAveragewordCount(MRJob):
    
    def mapper(self, _, line):
        (business_id, date, review_id, stars,other1)=line.split(',', 4)
        (other2, r_type, user_id, cool, useful, funny)=line.rsplit(',', 5)
        
        if business_id !="business_id":
            waste=r_type + user_id + cool + useful + funny
            other1=other1.rstrip(waste)
            other1=other1.translate(str.maketrans('', '', string.punctuation)).lower().split(" ")
        
            yield "words", len(other1)

    def reducer(self, key, values):
        a=0
        b=0
        for p in values:
            a +=p
            b +=1
        yield "average_words", a/b    

if __name__ == '__main__':
    MRAveragewordCount.run()
