from mrjob.job import MRJob

import csv


class TagFrequency(MRJob):


    def mapper(self, _, line):

        try:

            row = next(csv.reader([line]))

            if row[0] == 'userId':

                return

            tag = row[2].strip()

            yield tag, 1

        except Exception:

            pass


    def reducer(self, tag, counts):

        yield tag, sum(counts)


if __name__ == '__main__':

    TagFrequency.run()
