# A Knowledge Graph on Covid-19

The graph is available in a Neo4j Sandbox: https://10-0-1-172-33065.neo4jsandbox.com/browser/

**User:** public
**Password:** public

You can add it to Neo4j Desktop with the bolt URL, same user/password: bolt://100.24.206.62:33064

## Queries

Confirmed/death/recovered per country

```cypher
MATCH (c:Country)<-[:PART_OF]-(p:Province)-[:REPORTED]->(u:Update:Latest)
WHERE c.name = 'China'
RETURN c.name, sum(u.confirmed)
```

Confirmed cases for one country (percentage of the total population)

```cypher
MATCH (c:Country)-[r:CURRENT_TOTAL]->(:AgeGroup)
WHERE c.name = 'China'
WITH c, sum(r.count) AS total_population
MATCH (c)<-[:PART_OF]-(:Province)-[:REPORTED]->(u:Update:Latest)
WITH c.name AS Country, sum(u.confirmed) as Confirmed, max(u.date) AS Update, total_population AS Population
RETURN Country, Population, Confirmed, (toFloat(Confirmed)/Population)*100 AS percent
```


Confirmed cases per country (percentage of the total population)

```cypher
MATCH (c:Country)-[r:CURRENT_TOTAL]->(:AgeGroup)
WITH c, sum(r.count) AS total_population
MATCH (c)<-[:PART_OF]-(:Province)-[:REPORTED]->(u:Update:Latest)
WITH c.name AS Country, sum(u.confirmed) as Confirmed, max(u.date) AS Update, total_population AS Population
RETURN Country, Population, Confirmed, (toFloat(Confirmed)/Population)*100 AS percent ORDER BY percent DESC
```

Confirmed cases (total and percentage) for all entries for a country (one row for each province, many rows for China) -> query works only for countries that have **only one province**

```cypher
MATCH (c:Country)-[r:CURRENT_TOTAL]->(:AgeGroup)
WHERE c.name = 'Germany'
WITH c, sum(r.count) AS population
MATCH (c)<-[:PART_OF]-(p:Province)-[:REPORTED]->(u:Update)
RETURN DISTINCT c.name, p.name, u.date, population, u.confirmed, (toFloat(u.confirmed)/population)*100 AS percent ORDER BY u.date DESC LIMIT 10
```

Timeline of confirmed cases by country (aggregated over all provinces)

```cypher
MATCH (c:Country)<-[:PART_OF]-(:Province)-[:REPORTED]->(u:Update)
WHERE c.name = 'China'
WITH DISTINCT [u.date.year, u.date.month, u.date.day] AS date, sum(u.confirmed) AS sum
RETURN date, sum ORDER BY date
```

- problem: not every province for China is reported every day, thus the sum of all confirmed cases on data e.g. 18.03. does not equal the total nunber of confirmed cases at that date (because province X did report last on 17.03.) 

Number of people older than 70 in a country

```cypher
match (c:Country)-[r:CURRENT_TOTAL]-(ag:AgeGroup)
WHERE c.name = 'Germany' AND ag.start >= 70
RETURN sum(r.count)
```

## Datasources

### Covid-19 cases from John Hopkins University

John Hopkins University (JHU) aggregates data from WHO and other health organizations in a daily report. It contains the number of confirmed cases, deaths and recovered patients.

Dashboard: https://coronavirus.jhu.edu/map.html
Data: https://github.com/CSSEGISandData/COVID-19

### Population data from the UN

The UN gathers data on world population statistics and publishes the world population prospects: https://population.un.org/wpp/

The latest data set in CSV format can be found here: https://population.un.org/wpp/Download/Standard/CSV/


## Datamodel 
![Data Model](datamodel.png)