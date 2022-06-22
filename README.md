# faexport2
Reworking faexport, with some changes and improvements

## Planned functionality
- Scraper library and web API
- Typescript
  - With a python lib that wraps API access
- Support for multiple furry art sites
- Database cache, with data dumps

## Detailed design notes
### Data
- Common data and site specific data
### Scrapers
- Separate scrapers for different functionality, maybe?
- Separate scrapers for separate styles, I think?
- Scrapers can take fetchers as an argument, so that scrapers can share fetchers? Not sure
  - That way can have a standard FA fetcher, and a cloudflare bypass one
- API could have multiple scrapers for each site. Fallback through them. Could maybe get a database exporter one day, maybe (no)
