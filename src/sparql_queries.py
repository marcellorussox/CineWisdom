import time
from SPARQLWrapper import SPARQLWrapper, JSON


# Function to query Wikidata in batches and map IMDb IDs to Wikidata IDs
def query_wikidata_for_imdbid(imdb_ids, batch_size=25, sleep=1):
    """
    Map a list of IMDb IDs to Wikidata QIDs using batched SPARQL queries.

    Step-by-step:
    1. Define the Wikidata SPARQL endpoint.
    2. Split the list of IMDb IDs into batches to avoid server overload.
    3. For each batch:
        a. Construct a SPARQL query using VALUES to match multiple IMDb IDs.
        b. Execute the query using SPARQLWrapper.
        c. Extract the IMDb ID and corresponding Wikidata QID from the results.
        d. Store the mapping in a dictionary.
        e. Sleep for a short time between batches to avoid throttling.
    4. Return a dictionary mapping IMDb ID -> Wikidata QID.
    """
    WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
    mappings = {}

    # Helper generator to create batches from a list
    def batch(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i+size]

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


# Function to query DBpedia in batches and get English abstracts, directors, and actors
def query_dbpedia_for_data(wikidata_ids, batch_size=25, sleep=1):
    """
    Retrieve English-language abstracts, directors, and actors from DBpedia for a list of Wikidata QIDs.

    Step-by-step:
    1. Define the DBpedia SPARQL endpoint.
    2. Split the list of Wikidata QIDs into batches to avoid large queries.
    3. For each batch:
        a. Construct a SPARQL query using VALUES to match multiple QIDs.
        b. Include a UNION to handle Wikidata-to-DBpedia URL conversion if necessary.
        c. Request English abstracts, director names, and actor names.
        d. Execute the query and parse the JSON results.
        e. For each result:
            - Extract the Wikidata ID from the URI.
            - Add the abstract, director, and actors to a dictionary.
            - Ensure actors are stored as a list without duplicates.
        f. Sleep briefly between batches to reduce server load.
    4. Ensure all Wikidata IDs have an entry in the final dictionary.
    5. Return a dictionary mapping QID -> {abstract, director, actors}.
    """
    DBPEDIA_ENDPOINT = "http://dbpedia.org/sparql"
    data = {}

    # Helper generator to create batches from a list
    def batch(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i+size]

    # Loop over each batch
    for chunk in batch(wikidata_ids, batch_size):
        wd_values = " ".join([f"wd:{qid}" for qid in chunk])

        # SPARQL query to get English abstract, director, and actors
        dbpedia_query = f"""
        PREFIX dbo:  <http://dbpedia.org/ontology/>
        PREFIX wd:   <http://www.wikidata.org/entity/>
        PREFIX owl:  <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?wdId ?film ?abstract ?directorName ?actorName WHERE {{
          VALUES ?wdId {{ {wd_values} }}

          {{
            ?film owl:sameAs ?wdId .
          }}
          UNION
          {{
            BIND(IRI(CONCAT("http://wikidata.dbpedia.org/resource/",
                            STRAFTER(STR(?wdId), "http://www.wikidata.org/entity/"))) AS ?wdDb)
            ?film owl:sameAs ?wdDb .
          }}

          # abstract (English only)
          OPTIONAL {{ ?film dbo:abstract ?abstract . FILTER(LANG(?abstract) = "en") }}

          # director (English label only)
          OPTIONAL {{
            ?film dbo:director ?dir .
            ?dir rdfs:label ?directorName .
            FILTER(LANG(?directorName) = "en")
          }}

          # actors (English label only)
          OPTIONAL {{
            ?film dbo:starring ?actor .
            ?actor rdfs:label ?actorName .
            FILTER(LANG(?actorName) = "en")
          }}
        }}
        """

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

                abstract = b.get("abstract", {}).get("value")
                director = b.get("directorName", {}).get("value")
                actor = b.get("actorName", {}).get("value")

                if qid not in data:
                    data[qid] = {"abstract": abstract, "director": director, "actors": []}
                if actor and actor not in data[qid]["actors"]:
                    data[qid]["actors"].append(actor)

        except Exception as e:
            print(f"Error in DBpedia query (batch {chunk}): {e}")

        # Pause to respect server load
        time.sleep(sleep)

    # Ensure all Wikidata IDs have an entry
    for q in wikidata_ids:
        data.setdefault(q, {"abstract": None, "director": None, "actors": []})

    return data
