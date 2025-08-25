import re


class MappingManager:
    """
    Manages the mapping between Wikidata IDs and DBpedia data fields.
    """

    def __init__(self, dbpedia_data):
        self.dbpedia_data = dbpedia_data

    @staticmethod
    def clean_actor_name(name):
        """Clean an actor name by removing any text inside parentheses."""
        return re.sub(r"\s*\(.*?\)", "", name).strip()

    @staticmethod
    def format_list_field(items):
        """Clean and format list-based fields (genres, subjects, countries, languages)."""
        if items and isinstance(items, list):
            return "|".join(items)
        return None

    def get_title(self, wikidata_id):
        return self.dbpedia_data.get(wikidata_id, {}).get('title')

    def get_director(self, wikidata_id):
        return self.dbpedia_data.get(wikidata_id, {}).get('director')

    def get_actors(self, wikidata_id):
        actors = self.dbpedia_data.get(wikidata_id, {}).get('actors')
        return self.format_list_field(actors)

    def get_genres(self, wikidata_id):
        genres = self.dbpedia_data.get(wikidata_id, {}).get('genres', [])
        return self.format_list_field(genres)

    def get_subjects(self, wikidata_id):
        subjects = self.dbpedia_data.get(wikidata_id, {}).get('subjects', [])
        return self.format_list_field(subjects)

    def get_release_date(self, wikidata_id):
        return self.dbpedia_data.get(wikidata_id, {}).get('releaseDate')

    def get_runtime(self, wikidata_id):
        return self.dbpedia_data.get(wikidata_id, {}).get('runtime')

    def get_countries(self, wikidata_id):
        countries = self.dbpedia_data.get(wikidata_id, {}).get('countries', [])
        return self.format_list_field(countries)

    def get_languages(self, wikidata_id):
        languages = self.dbpedia_data.get(wikidata_id, {}).get('languages', [])
        return self.format_list_field(languages)

    def get_story(self, wikidata_id):
        return self.dbpedia_data.get(wikidata_id, {}).get('story')

    def get_theme(self, wikidata_id):
        return self.dbpedia_data.get(wikidata_id, {}).get('theme')

    def get_abstract(self, wikidata_id):
        abstract = self.dbpedia_data.get(wikidata_id, {}).get('abstract')
        if abstract and isinstance(abstract, str):
            return abstract.strip('"')
        return abstract
