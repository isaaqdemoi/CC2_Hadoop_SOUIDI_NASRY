from mrjob.job import MRJob

import csv


class TagsPerUserMovie(MRJob):


    def mapper(self, _, line):

        try:

            row = next(csv.reader([line]))

            if row[0] == 'userId':

                return

            movieID = row[1]

            userID = row[0]

            yield (movieID, userID), 1

        except Exception:

            pass


    def reducer(self, key, counts):

        yield key, sum(counts)


if __name__ == '__main__':

    TagsPerUserMovie.run()
