from mrjob.job import MRJob

import csv


class TagsPerMovie(MRJob):


    def mapper(self, _, line):

        try:

            row = next(csv.reader([line]))

            if row[0] == 'userId':

                return

            movieID = row[1]

            yield movieID, 1

        except Exception:

            pass


    def reducer(self, movieID, counts):

        yield movieID, sum(counts)


if __name__ == '__main__':

    TagsPerMovie.run()
