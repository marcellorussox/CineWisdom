"""
Template for SPARQL queries used in the project.
This template uses a federated query approach to link Wikidata and DBpedia.
"""

DBPEDIA_MOVIE_QUERY = """
PREFIX dbo:  <http://dbpedia.org/ontology/>
PREFIX wd:   <http://www.wikidata.org/entity/>
PREFIX wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dct:  <http://purl.org/dc/terms/>
PREFIX dbp:  <http://dbpedia.org/property/>

SELECT ?wdId ?film ?abstract ?directorName ?actorName ?title ?genre 
       ?subject ?releaseDate ?runtime ?country ?language ?story ?theme
WHERE {{
  VALUES ?wdId {{ {wd_values} }}
  
  # Federated query: ask the DBpedia endpoint to search for the Wikidata ID via owl:sameAs
  ?film owl:sameAs ?wdId .

  # Title (English only)
  OPTIONAL {{ 
    ?film rdfs:label ?title . 
    FILTER(LANG(?title) = "en")
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

  # genre (English label only)
  OPTIONAL {{
    ?film dbo:genre ?genreRes .
    ?genreRes rdfs:label ?genre .
    FILTER(LANG(?genre) = "en")
  }}

  # runtime
  OPTIONAL {{
    ?film dbo:runtime ?runtime .
  }}
}}
"""