from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        for word in line.lower().split():
            yield (word, 1)

    def combiner(self, word, aggregated_counts):
        yield word, sum(aggregated_counts)

    def reducer(self, key, count):
        yield key, sum(count)


if __name__ == '__main__':
    MRWordFrequencyCount.run()