import json
import numpy as np
from random import randrange, seed
from mrjob.job import MRJob
from mrjob.step import MRStep
from sklearn.feature_extraction.text import TfidfVectorizer
from utils import Utils


def cosine_distance(summary_a, summary_b):
    """
    Computes the cosine similarity score
    :param summary_a: string containing all the words in the random paper
    :param summary_b: string containing all the words in the every paper
    :return: returns the Cosine coefficient score, how similar are both sets
    """
    dot_product = np.dot(summary_a, summary_b)
    norm_summary_a = np.linalg.norm(summary_a)
    norm_summary_b = np.linalg.norm(summary_b)
    cos = dot_product / (norm_summary_a * norm_summary_b)
    return cos


def string_to_set(text):
    """
    Eliminates stop words in a string and returns a clean set
    :param text: text to be cleaned
    :return: clean set
    """
    clean = Utils.tokenize_words(text)
    clean_map = set(list(map(lambda x: x[0], clean)))
    return clean_map


class MRCosineSimilarityScore(MRJob):

    def steps(self):
        return [
            MRStep(mapper_raw=self.extract_entities,
                   reducer=self.cosine_reducer)
        ]


    #         Mapper which receives the path of the json file, selects a random summmary and computes the cosine similarity
    #         score by iterating all the other summaries
    #         :param json_path: path of the json file, this is important for the mapper_raw
    #         :param json_uri: path of the json file, this is important for the mapper_raw
    def extract_entities(self, json_path, json_uri):
        vectorizer = TfidfVectorizer()
        js = json.load(open(json_path))
        seed(45)  # Set seed
        random_number = randrange(0, len(js) - 1, 1)  # Get random number between 0 and length of json array
        random_summary = js[random_number]["summary"]  # Get corresponding random summary
        random_summary = random_summary.replace('\n', ' ')  # Line added only for visual purposes, the real filter is in Utils
        random_id = js[random_number]["id"]  # Get id of random summary
        set_random_summary = string_to_set(random_summary)  # Filter stop words from random summary
        string_random_summary = " ".join(set_random_summary)  # Transform set into string

        for entity in js:
            entity["summary"] = entity["summary"].replace('\n', ' ')  # Line added only for visual purposes, the real filter is in Utils
            set_summary = string_to_set(entity["summary"])  # Filter stop words from summary
            string_summary = " ".join(set_summary)  # Transform set into string
            vectors = vectorizer.fit_transform([string_random_summary, string_summary])  # Vectorize summary using Tfid
            dense = vectors.todense()
            dense_list = dense.tolist()  # Get lists
            summary_random = dense_list[0]
            summary = dense_list[1]
            cosine_similarity = cosine_distance(summary_random, summary)  # Get cosine similarity score
            if entity["id"] != random_id:  # Filter same id of random summary
                yield random_summary, (cosine_similarity, entity["summary"])

    def cosine_reducer(self, random_summary, entity):
        """
        Reducer that gets max cosine similarity score
        :param random_summary: summary of the random paper
        :param entity: distance of every paper in the file and its summary
        """
        yield random_summary, max(entity)


if __name__ == '__main__':
    MRCosineSimilarityScore.run()
