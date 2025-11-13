import time
import functools
from SPARQLWrapper import SPARQLWrapper, JSON
from src.data.templates import DBPEDIA_MOVIE_QUERY

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
DBPEDIA_ENDPOINT = "https://dbpedia.org/sparql"

# 🚀 PERFORMANCE: Cache per SPARQL queries (evita chiamate duplicate)
# Evita di fare la stessa query SPARQL più volte (le API sono lente!)
_sparql_cache = {}


def _get_cache_key(query: str, params: tuple) -> str:
    """Generate cache key for SPARQL query."""
    return f"{hash(query)}_{hash(params)}"


def cached_sparql_query(query_func):
    """
    🚀 OPTIMIZED: Decorator per caching SPARQL queries.

    Usage:
        @cached_sparql_query
        def query_function(...):
            ...
    """
    @functools.wraps(query_func)
    def wrapper(*args, **kwargs):
        # Create cache key from function name and arguments
        cache_key = _get_cache_key(query_func.__name__, (args, tuple(sorted(kwargs.items()))))

        # Check cache first
        if cache_key in _sparql_cache:
            return _sparql_cache[cache_key]

        # Cache miss: execute query and store result
        result = query_func(*args, **kwargs)
        _sparql_cache[cache_key] = result
        return result

    return wrapper


def clear_sparql_cache():
    """Clear SPARQL cache if needed."""
    global _sparql_cache
    _sparql_cache.clear()
    print("SPARQL cache cleared.")


# Function to query Wikidata in batches and map IMDb IDs to Wikidata IDs
@cached_sparql_query
def query_wikidata_for_imdbid(imdb_ids, batch_size=25, sleep=1, max_retries=3):
    """
    🚀 OPTIMIZED: Map a list of IMDb IDs to Wikidata QIDs using batched SPARQL queries with retries.

    Uses caching to avoid duplicate queries to slow SPARQL endpoints.
    """
    mappings = {}

    def batch(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    for chunk in batch(imdb_ids, batch_size):
        imdb_values = " ".join([f'"{i}"' for i in chunk])
        query = f"""
        SELECT ?item ?imdbId WHERE {{
          VALUES ?imdbId {{ {imdb_values} }}
          ?item wdt:P345 ?imdbId .
        }}
        """

        sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)

        for attempt in range(max_retries):
            try:
                results = sparql.query().convert()
                bindings = results.get("results", {}).get("bindings", [])

                for b in bindings:
                    imdb_id = b.get("imdbId", {}).get("value")
                    wikidata_id = b.get("item", {}).get("value", "").split("/")[-1]
                    if imdb_id and wikidata_id:
                        mappings[imdb_id] = wikidata_id

                # Success, break out of the retry loop
                break
            except Exception as e:
                print(f"[ERRORE] Tentativo {attempt + 1}/{max_retries} fallito per il batch: {chunk}. Errore: {e}")
                time.sleep(sleep * (attempt + 1))  # Exponential backoff

        time.sleep(sleep)

    return mappings


# Function to query DBpedia in batches and get additional information
@cached_sparql_query
def query_dbpedia_for_data(wikidata_ids, batch_size=25, sleep=1, max_retries=3):
    data = {}

    def batch(iterable, size):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

    single_value_fields = {
        'title': 'title',
        'directorName': 'director',
        'runtime': 'runtime',
        'abstract': 'abstract'
    }
    list_fields = {
        'actorName': 'actors',
    }

    for chunk in batch(wikidata_ids, batch_size):
        wd_values = " ".join([f"wd:{qid}" for qid in chunk])
        dbpedia_query = DBPEDIA_MOVIE_QUERY.format(wd_values=wd_values)

        sparql = SPARQLWrapper(DBPEDIA_ENDPOINT)
        sparql.setQuery(dbpedia_query)
        sparql.setReturnFormat(JSON)

        for attempt in range(max_retries):
            try:
                results = sparql.query().convert()
                bindings = results.get("results", {}).get("bindings", [])

                for b in bindings:
                    wd_uri = b.get("wdId", {}).get("value", "")
                    qid = wd_uri.rsplit("/", 1)[-1] if wd_uri else None
                    if not qid:
                        continue

                    if qid not in data:
                        data[qid] = {
                            "title": None, "director": None, "runtime": None, "actors": [], "abstract": None
                        }

                    for sparql_key, data_key in single_value_fields.items():
                        value = b.get(sparql_key, {}).get("value")
                        if value:
                            data[qid][data_key] = value

                    for source_field, target_field in list_fields.items():
                        value = b.get(source_field, {}).get("value")
                        if value and value not in data[qid][target_field]:
                            data[qid][target_field].append(value)

                # Success, break out of the retry loop
                break
            except Exception as e:
                print(f"[ERRORE] Tentativo {attempt + 1}/{max_retries} fallito per il batch: {chunk}. Errore: {e}")
                time.sleep(sleep * (attempt + 1))  # Exponential backoff

        time.sleep(sleep)

    default_entry = {
        "title": None, "director": None, "runtime": None, "actors": [], "abstract": None
    }
    for q in wikidata_ids:
        data.setdefault(q, default_entry.copy())
    return data
