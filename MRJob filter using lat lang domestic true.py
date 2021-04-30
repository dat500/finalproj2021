from mrjob.job import MRJob
import mrjob
import pdb
class MRFilter(MRJob):
    # We want raw value only output - no key
    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol

    def mapper(self, _, line):
        # Each line is CSV
        # Skip header and output just 1st Feb
        l = [s.strip('"') for s in line.split(',')]
        flag=True
        try:
            float(l[19])
            float(l[20])
            if ( float(l[19]) >=40  and float(l[19])<45 and float(l[20]) >=-90  and float(l[20])<-85 and l[9]=="true" ):
            # strip off quotes
                yield line, None
        except:
            yield None, None

    def reducer(self, lines, _):
        yield None, lines

if __name__ == '__main__':
    MRFilter.run()
