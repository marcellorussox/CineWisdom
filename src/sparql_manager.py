import time
from SPARQLWrapper import SPARQLWrapper, JSON
from .sparql_template import DBPEDIA_MOVIE_QUERY

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
DBPEDIA_ENDPOINT = "https://dbpedia.org/sparql"


# Function to query Wikidata in batches and map IMDb IDs to Wikidata IDs
def query_wikidata_for_imdbid(imdb_ids, batch_size=25, sleep=1):
    """
    Map a list of IMDb IDs to Wikidata QIDs using batched SPARQL queries.

    Step-by-step:
    1. Split the list of IMDb IDs into batches to avoid server overload.
    2. For each batch:
        a. Construct a SPARQL query using VALUES to match multiple IMDb IDs.
        b. Execute the query using SPARQLWrapper.
        c. Extract the IMDb ID and corresponding Wikidata QID from the results.
        d. Store the mapping in a dictionary.
        e. Sleep for a short time between batches to avoid throttling.
    3. Return a dictionary mapping IMDb ID -> Wikidata QID.
    """
    mappings = {}

    # Helper generator to create batches from a list
    def batch(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    # Loop over each batch
    for chunk in batch(imdb_ids, batch_size):
        # Format batch values for SPARQL VALUES clause
        imdb_values = " ".join([f'"{i}"' for i in chunk])

        # SPARQL query to get Wikidata QIDs for each IMDb ID
        query = f"""
        SELECT ?item ?imdbId WHERE {{
          VALUES ?imdbId {{ {imdb_values} }}
          ?item wdt:P345 ?imdbId .
        }}
        """

        sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        # Execute query and parse results
        try:
            results = sparql.query().convert()
            for b in results.get("results", {}).get("bindings", []):
                imdb_id = b.get("imdbId", {}).get("value")
                wikidata_id = b.get("item", {}).get("value", "").split("/")[-1]
                if imdb_id and wikidata_id:
                    mappings[imdb_id] = wikidata_id
        except Exception as e:
            print(f"Error in Wikidata query (batch {chunk}): {e}")

        # Pause to respect server load
        time.sleep(sleep)

    return mappings


# Function to query DBpedia in batches and get additional information
def query_dbpedia_for_data(wikidata_ids, batch_size=25, sleep=1):
    """
    Retrieve comprehensive movie data from DBpedia for a list of Wikidata QIDs.

    Step-by-step:
    1. Split the list of Wikidata QIDs into batches to avoid large queries.
    2. For each batch:
        a. Construct a SPARQL query using VALUES to match multiple QIDs.
        b. Include a UNION to handle Wikidata-to-DBpedia URL conversion if necessary.
        c. Request comprehensive movie data including title, director, actors, genres, etc.
        d. Execute the query and parse the JSON results.
        e. For each result:
            - Extract the Wikidata ID from the URI.
            - Add all extracted data to a dictionary, ensuring list fields don't contain duplicates.
        f. Sleep briefly between batches to reduce server load.
    3. Ensure all Wikidata IDs have an entry in the final dictionary.
    4. Return a dictionary mapping QID -> comprehensive movie data.
    """
    DBPEDIA_ENDPOINT = "http://dbpedia.org/sparql"
    data = {}

    # Helper generator to create batches from a list
    def batch(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    # Define field mappings for list-based fields
    list_fields = {
        'actorName': 'actors',
        'genre': 'genres',
        'subject': 'subjects',
        'country': 'countries',
        'language': 'languages'
    }

    # Loop over each batch
    for chunk in batch(wikidata_ids, batch_size):
        wd_values = " ".join([f"wd:{qid}" for qid in chunk])

        # Enhanced SPARQL query to get additional information
        dbpedia_query = DBPEDIA_MOVIE_QUERY.format(wd_values=wd_values)

        sparql = SPARQLWrapper(DBPEDIA_ENDPOINT)
        sparql.setQuery(dbpedia_query)
        sparql.setReturnFormat(JSON)

        # Execute query and parse results
        try:
            results = sparql.query().convert()
            for b in results.get("results", {}).get("bindings", []):
                wd_uri = b.get("wdId", {}).get("value", "")
                qid = wd_uri.rsplit("/", 1)[-1] if wd_uri else None
                if not qid:
                    continue

                # Extract all values
                title = b.get("title", {}).get("value")
                director = b.get("directorName", {}).get("value")
                release_date = b.get("releaseDate", {}).get("value")
                runtime = b.get("runtime", {}).get("value")
                story = b.get("story", {}).get("value")
                theme = b.get("theme", {}).get("value")
                abstract = b.get("abstract", {}).get("value")

                if qid not in data:
                    data[qid] = {
                        "title": title,
                        "director": director,
                        "actors": [],
                        "genres": [],
                        "subjects": [],
                        "releaseDate": release_date,
                        "runtime": runtime,
                        "countries": [],
                        "languages": [],
                        "story": story,
                        "theme": theme,
                        "abstract": abstract
                    }

                # Process list-based fields using the mapping
                for source_field, target_field in list_fields.items():
                    value = b.get(source_field, {}).get("value")
                    if value and value not in data[qid][target_field]:
                        data[qid][target_field].append(value)

        except Exception as e:
            print(f"Error in DBpedia query (batch {chunk}): {e}")

        # Pause to respect server load
        time.sleep(sleep)

    # Ensure all Wikidata IDs have an entry with all fields
    default_entry = {
        "title": None,
        "director": None,
        "actors": [],
        "genres": [],
        "subjects": [],
        "releaseDate": None,
        "runtime": None,
        "countries": [],
        "languages": [],
        "story": None,
        "theme": None,
        "abstract": None
    }

    for q in wikidata_ids:
        data.setdefault(q, default_entry.copy())

    return data
