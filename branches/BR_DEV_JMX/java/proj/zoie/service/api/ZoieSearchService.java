package proj.zoie.service.api;

import proj.zoie.api.ZoieException;

public interface ZoieSearchService {
	SearchResult search(SearchRequest req) throws ZoieException;
}
