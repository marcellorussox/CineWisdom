import time
from SPARQLWrapper import SPARQLWrapper, JSON


# TODO le query non funzionano a dovere

def query_wikidata_for_imdbid(imdb_ids):
    """
    Esegue una query batched a Wikidata per mappare gli ID di IMDb a ID di Wikidata.

    Parameters:
    - imdb_ids (list): Una lista di ID IMDb.

    Returns:
    - dict: Una mappatura degli ID IMDb a ID Wikidata.
    """
    WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"

    wikidata_query = f"""
    SELECT ?item ?imdbId WHERE {{ 
      VALUES ?imdbId {{ {' '.join([f'"{i}"' for i in imdb_ids])} }}
      ?item wdt:P345 ?imdbId. 
    }}
    """

    sparql_wikidata = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql_wikidata.setQuery(wikidata_query)
    sparql_wikidata.setReturnFormat(JSON)

    try:
        results = sparql_wikidata.query().convert()
        wikidata_mappings = {}
        for binding in results['results']['bindings']:
            imdb_id = binding['imdbId']['value']
            wikidata_id = binding['item']['value'].split('/')[-1]
            wikidata_mappings[imdb_id] = wikidata_id
        return wikidata_mappings
    except Exception as e:
        print(f"Errore nella query Wikidata: {e}")
        return {}


def query_dbpedia_for_data(wikidata_ids):
    """
    Esegue una query batched a DBpedia per ottenere dati sui film usando gli ID Wikidata.

    Parameters:
    - wikidata_ids (list): Una lista di ID Wikidata.

    Returns:
    - dict: Un dizionario che mappa gli ID Wikidata ai dati (abstract, director).
    """
    DBPEDIA_ENDPOINT = "http://dbpedia.org/sparql"

    dbpedia_query = f"""
    PREFIX dbo: <http://dbpedia.org/ontology/>
    PREFIX dbr: <http://dbpedia.org/resource/>
    PREFIX wd: <http://www.wikidata.org/entity/>

    SELECT ?film ?abstract ?director WHERE {{
      VALUES ?wdId {{ {' '.join([f'wd:{f}' for f in wikidata_ids])} }}
      ?film owl:sameAs ?wdId .
      OPTIONAL {{ ?film dbo:abstract ?abstract . FILTER(lang(?abstract) = "en") }}
      OPTIONAL {{ ?film dbo:director ?directorResource . ?directorResource rdfs:label ?director . FILTER(lang(?director) = "en") }}
    }}
    """

    sparql_dbpedia = SPARQLWrapper(DBPEDIA_ENDPOINT)
    sparql_dbpedia.setQuery(dbpedia_query)
    sparql_dbpedia.setReturnFormat(JSON)

    try:
        results = sparql_dbpedia.query().convert()
        dbpedia_data = {}
        for binding in results['results']['bindings']:
            film_uri = binding['film']['value']
            wikidata_id = film_uri.split('http://www.wikidata.org/entity/')[-1]
            abstract = binding['abstract']['value'] if 'abstract' in binding else None
            director = binding['director']['value'] if 'director' in binding else None
            dbpedia_data[wikidata_id] = {'abstract': abstract, 'director': director}
        return dbpedia_data
    except Exception as e:
        print(f"Errore nella query DBpedia: {e}")
        return {}
