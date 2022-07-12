# faexport DB
The original goal of this project is to act as some sort of cache database for various furry art sites.

I'm writing out this Readme because I got a bit lost on what this project is meant to achieve and why.

## Data ingest
- I want [FAExport](https://github.com/Deer-Spangle/FAExport) to be able to push submission and user scrape data to this database.
- I was thinking it would also be great to import other data dumps I have been given, from fa-indexer, findfurrypicbot and fuzzysearch and such.
- Might be cool if other projects could push to it too, if it were generic enough for that.
- I'm not sure whether FAExport would push in FAExport format, or reformat to push to this project. The former might be best.
- Data dumps might not be ingested live from when they were scraped, so it seems important to be able to tell which value is the current one for a given submission, and maybe keep the old ones? (Old file data at least, see above)

## Data model
- I figured it would be neat for this project to support multiple furry art sites.
- The data model needs to be flexible to dumps which contain a flexible subset of a submission's possible data.
  - I do want to store all that extra stuff, with a mind to expanding to more core stuff, so using json
- Art sites on my radar are: Furaffinity, weasyl, e621, furry network, sofurry, inkbunny, f-list
- Some websites support multiple files per submission, so that seems necessary as a result
- I was not planning to store the actual files uploaded to these sites, as that seems like a massive storage burden and risk.
- Keywords on some websites (e621) are unordered and unique, while other websites (FA) allow non-unique keywords, and keep an order
  - I also included extra data on keywords, in case a keyword assigned to a submission might be allowed extra data? (Maybe datetime it was added? User-submitted vs artist-added? Or something)
- When a file is updated, the hashes will be invalidated, but it seems useful to keep the old hashes, such that image matches can say they used to match a given submission?


## Potential uses
- Maybe FASearchBot could use it when FA is down?
- Maybe users could download bulk data dumps, to prevent needing to scrape those websites
  - Either of the entire database, or of things like username lists, which are not otherwise available
- Maybe there could be a search endpoint to allow searching all sites by keyword? I pulled the title, description, and keywords out of bulk submission data for this reason
- Maybe there could be a hash lookup? For file hashes and image hashes

## Thoughts
- Maybe it would be good to store a list of ingested data somewhere, so someone else could subscribe to the feed?
- Maybe it would be better to store the ingested data as separate entries, and just merge them down into one submission when it is requested?
- Maybe it would be better to do ingestion via something async like kafka, rather than http

## Todo:
- Web interface
  - Dockerise
  - For users to access cached data for an entry
  - For faexport to push data into
  - For users to download database exports
- Logging
- Prometheus
- Database schema setup and migrations

### Possible endpoints
- GET /api/view/submissions/fa/3748252.json
  - View that submission
- GET /api/view/submissions/fa/3748252/snapshots [TODO]
  - View the snapshots that make up that submission
- POST /api/ingest/faexport/submission [TODO]
  - Post a submission to ingest it into the database
- POST /api/ingest/faexport/user [TODO]
  - Post a user to ingest it into the database
- GET /api/view/users/fa/dr-spangle.json
  - View a user data
- GET /api/view/users/fa.json
  - List all user IDs for site?
- POST /api/hash_search/<algo_id> [TODO]
  - Post hash, get a list of matching submissions?
- POST /api/hash_search/<algo_lang>/<algo_name> [TODO]
  - Post hash, get a list of matching submissions?
- GET /api/websites.json
  - List websites
- GET /api/hash_algos.json
  - List hash algorithms
