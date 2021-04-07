from mrjob.job import MRJob
from mrjob.step import MRStep


class MRTerroristsAttacks(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_relevant_pairs,  # Get (Region, Country) 
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_country_count_per_region),
            MRStep(reducer=self.reducer_sorter),
        ]

    def mapper_get_relevant_pairs(self, _, row):
        list_of_params = row.split(',')

        if list_of_params[0] != 'iyear' and 1980 <= int(list_of_params[0]) <= 2000:
            projection = (list_of_params[9], list_of_params[7])
            yield projection, 1

    def reducer_count_words(self, projection, counts):
        yield projection[0], (sum(counts), projection[1],projection[0])

    def reducer_find_max_country_count_per_region(self, region, country_data):
        yield None, max(country_data)

    def reducer_sorter(self, _, country):
        sorted_countries = sorted(country, reverse=True)
        for country in sorted_countries:
            yield country[2], country[1]


def main():
    MRTerroristsAttacks.run()


if __name__ == "__main__":
    main()
