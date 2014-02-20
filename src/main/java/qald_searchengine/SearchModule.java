package qald_searchengine;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase.HitContext;

public class SearchModule {

	public static void main(String[] args) {
		// TODO Auto-generated method stub


		Client client =getClient();
	
		SearchResponse response = client.prepareSearch("twitter")
		        .setTypes("type1", "type2")
		        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		        .setQuery(QueryBuilders.termQuery("aa", "kyungtae"))             // Query
		 //       .setPostFilter(FilterBuilders.rangeFilter("age").from(12).to(18))   // Filter
		        .setFrom(0).setSize(60).setExplain(true)
		        .execute()
		        .actionGet();


		SearchHit[] results = response.getHits().getHits();
		for (SearchHit hit : results) {
		  System.out.println(hit.getId());    //prints out the id of the document
		  System.out.println(hit.getScore());
		  Map<String,Object> result = hit.getSource();   //the retrieved document

		}
	}
	
	
	
/*	public static void scrolls(){ 
		
		Client client =getClient();
		
		QueryBuilder qb = termQuery("multi", "test");

		SearchResponse scrollResp = client.prepareSearch(test)
		        .setSearchType(SearchType.SCAN)
		        .setScroll(new TimeValue(60000))
		        .setQuery(qb)
		        .setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
		//Scroll until no hits are returned
		while (true) {
		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
		    for (SearchHit hit : scrollResp.getHits()) {
		        //Handle the hit...
		    }
		    //Break condition: No hits are returned
		    if (scrollResp.getHits().getHits().length == 0) {
		        break;
		    }
		}
	}
	*/
	
	
	//Create Client 
	public static Client getClient(){ 
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "okbqasearch").build(); 
		TransportClient transportClient = new TransportClient(settings); 
		transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress("143.248.135.212", 9300));  
		return (Client) transportClient; 
	}

}
