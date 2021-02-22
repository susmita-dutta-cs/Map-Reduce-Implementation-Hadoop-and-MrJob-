from mrjob.job import MRJob
from mrjob.step import MRStep
from random import randrange, seed
from utils import Utils
import json


# Computes the Jaccard Similarity Coefficient
#     :param xs: set containing all the words in the random paper
#     :param ys: set containing all the words in the every paper
#     :return: return the Jaccard index, how similar are both sets

def jaccard(xs, ys):
      value = float(len(set(xs) & set(ys))) / len(set(xs) | set(ys))
      return value
     # return float(len(set(xs) & set(ys))) / len(set(xs) | set(ys))


def string_to_set(text):
    """
    Eliminates stop words in a string and returns a clean set
    :param text: text to be cleaned
    :return: clean set
    """
    clean = Utils.tokenize_words(text)
    clean_map = set(list(map(lambda x: x[0], clean)))
    return clean_map


class MRJaccardCoefficient(MRJob):

    def steps(self):
        return [
            MRStep(mapper_raw=self.extract_entities,
                   reducer=self.jaccard_reducer)
        ]

    def extract_entities(self, json_path, json_uri):
        """
        Mapper which receives the path of the json file, selects a randomly a paper, use its summmary and computes the
        jaccard index by iterating all the other summaries
        :param json_path: path of the json file, this is important for the mapper_raw
        :param json_uri: path of the json file, this is important for the mapper_raw
        """
        js = json.load(open(json_path))
        seed(45)  # Set seed
        random_number = randrange(0, len(js) - 1, 1)  # Get random number between 0 and length of json array
        random_summary = js[random_number]["summary"]  # Get corresponding random summary
        random_summary = random_summary.replace('\n', ' ')  # Line added only for visual purposes, the real filter is in Utils
        random_id = js[random_number]["id"]  # Get id of random summary
        set_random_summary = string_to_set(random_summary)  # Filter stop words from random summary

        for entity in js:
            entity["summary"] = entity["summary"].replace('\n', ' ')  # Line added only for visual purposes, the real filter is in Utils
            set_summary = string_to_set(entity["summary"])  # Filter stop words from summary
            distance = jaccard(set_random_summary, set_summary)  # Get Jaccard index
            if entity["id"] != random_id:
                yield random_summary, (distance, entity["summary"])

    def jaccard_reducer(self, random_summary, entity):
        """
        Reducer that gets max jaccard index
        :param random_summary: summary of the random paper
        :param entity: distance of every paper in the file and its summary
        """
        yield random_summary, max(entity)


if __name__ == '__main__':
    MRJaccardCoefficient.run()
