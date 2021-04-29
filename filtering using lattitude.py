rom mrjob.job import MRJob
import mrjob
import pdb

class MRFilter(MRJob):

    def mapper(self, _, line):
        # Each line is CSV
        # Skip header and output just 1st Feb
        l = [s.strip('"') for s in line.split(',')]
        flag=True
        try:
            float(l[19])
            if ( float(l[19]) >=41  and float(l[19])<42.1):
            # strip off quotes
                yield line.replace('"',''), None

        except:
            yield None, None


    def reducer(self, lines, _):
        yield None, lines

if __name__ == '__main__':
    MRFilter.run()
                     