"""
Template for SPARQL queries used in the projecy
"""

DBPEDIA_MOVIE_QUERY = """
PREFIX dbo:  <https://dbpedia.org/ontology/>
PREFIX wd:   <https://www.wikidata.org/entity/>
PREFIX owl:  <https://www.w3.org/2002/07/owl#>
PREFIX rdfs: <https://www.w3.org/2000/01/rdf-schema#>
PREFIX dct:  <https://purl.org/dc/terms/>
PREFIX dbp:  <https://dbpedia.org/property/>

SELECT ?wdId ?film ?abstract ?directorName ?actorName ?title ?genre 
       ?subject ?releaseDate ?runtime ?country ?language ?story ?theme 
WHERE {{
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

  # subject (English label only)
  OPTIONAL {{
    ?film dct:subject ?subjectRes .
    ?subjectRes rdfs:label ?subject .
    FILTER(LANG(?subject) = "en")
  }}

  # releaseDate
  OPTIONAL {{
    ?film dbo:releaseDate ?releaseDate .
  }}

  # runtime
  OPTIONAL {{
    ?film dbo:runtime ?runtime .
  }}

  # country (English label only)
  OPTIONAL {{
    ?film dbo:country ?countryRes .
    ?countryRes rdfs:label ?country .
    FILTER(LANG(?country) = "en")
  }}

  # language (English label only)
  OPTIONAL {{
    ?film dbo:language ?languageRes .
    ?languageRes rdfs:label ?language .
    FILTER(LANG(?language) = "en")
  }}

  # story (English only)
  OPTIONAL {{
    ?film dbp:story ?story .
    FILTER(LANG(?story) = "en")
  }}

  # theme (English only)
  OPTIONAL {{
    ?film dbp:theme ?theme .
    FILTER(LANG(?theme) = "en")
  }}
}}
"""
