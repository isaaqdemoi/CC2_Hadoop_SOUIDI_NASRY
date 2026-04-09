from mrjob.job import MRJob

import csv


class TagsPerUser(MRJob):


    def mapper(self, _, line):

        try:

            row = next(csv.reader([line]))

            if row[0] == 'userId':

                return

            userID = row[0]

            yield userID, 1

        except Exception:

            pass


    def reducer(self, userID, counts):

        yield userID, sum(counts)


if __name__ == '__main__':

    TagsPerUser.run()
