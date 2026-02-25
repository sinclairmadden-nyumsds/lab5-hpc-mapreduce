from mrjob.job import MRJob
import csv


class MRSalesPerCategory(MRJob):

    def mapper(self, _, line):
        if "user_id" in line:
            return

        row = next(csv.reader([line]))
        category = row[3]
        yield category, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    MRSalesPerCategory.run()
