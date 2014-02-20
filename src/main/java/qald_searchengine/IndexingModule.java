package qald_searchengine;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

public class IndexingModule {

    String clientIp = "143.248.135.212";
    int port = 9300;
    String documentType = "label";
    String shardName = "okbqasearch";
    String indexName = "dbpediasurfaceform";
    Set<String> whiteList = null;
    public static int bulkSize = 1000;
    public static final Logger log = Logger.getLogger("OPENQA");
    public int numberOfSearchResults = 10;
    public boolean verbose = true;
    public static void main(String[] args) {
        //testIndex();
        testSearch();
        // TODO Auto-generated method stub
        // on startup
        if (args.length > 0) {
            try {

                IndexingModule im = new IndexingModule();
                //im.insertToIndexByFile("C:/workspace/qald_searchengine/dictionaryList/enTest.txt");

                if (args.length > 1) {
                    im.shardName = args[1];
                }
                if (args.length > 2) {
                    im.clientIp = args[2];
                    im.port = Integer.parseInt(args[3]);
                    im.shardName = args[4];
                }
                if (args.length > 5) {
                    im.whiteList = getWhiteList(args[5]);
                }
                im.insertToIndexByFile(args[0]);

            } catch (ElasticsearchException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    /**
     * Reads the white list from a file (one URI per line)
     *
     * @param fileName File containing the URIs
     * @return List of URIs whose surface forms are to be indexed
     * @throws IOException If file cannot be read
     */
    public static Set<String> getWhiteList(String fileName) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
        Set<String> whiteList = new HashSet<String>();
        String str;
        while ((str = in.readLine()) != null) {
            whiteList.add(str);
        }
        return whiteList;
    }

    /**
     * Reads a TSV file of surface forms and write it in the index. Loads in
     * bulk.
     *
     * @param fileName TSV File
     * @throws ElasticsearchException If server does not work
     * @throws IOException If file cannot be read
     */
    public void insertToIndexByFile(String fileName) throws ElasticsearchException, IOException {
        Client client = getClient();
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
        String uri, surfaceForm, str;
        int index = 1;
        boolean indexing;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        BulkResponse response;
        while ((str = in.readLine()) != null) {
            String[] strArr = str.split("\t");
            uri = strArr[0];
            indexing = false;
            if (whiteList == null) {
                indexing = true;
            } else if (whiteList.contains(uri)) {
                indexing = true;
            }
            if (indexing) {
                for (int i = 1; i < strArr.length; i++) {
                    surfaceForm = strArr[i];
                    Map<String, Object> json = new HashMap<String, Object>();
                    json.put("uri", uri);
                    json.put("surfaceForm", surfaceForm);
                    json.put("type", "Entity");

                    //index plays the role of the ID value
                    bulkRequest.add(client.prepareIndex(shardName, documentType, "" + index)
                            .setSource(json));
                    if(verbose) log.log(Level.INFO, "Inserting {0}", json);
                    index++;
                    if (index % bulkSize == 0) {
                        response = bulkRequest.execute().actionGet();
                        if (response.hasFailures()) {
                            log.warning(response.buildFailureMessage());
                        }
                    }
                }
            }
            if (bulkRequest.numberOfActions() > 0) {
                response = bulkRequest.execute().actionGet();
                if (response.hasFailures()) {
                    log.warning(response.buildFailureMessage());
                }
            }
        }
        client.close();
    }

    /**
     * Connect to the server and pick the index where the file is to be written
     *
     * @return Client for indexing
     */
    public Client getClient() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", shardName).build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(clientIp, port));
        return (Client) transportClient;
    }

    public Map<String, Float> searchIndex(String searchString) {
        Map<String, Float> result;
        result = new HashMap<String, Float>();
        Client client = getClient();
        SearchResponse response = client.prepareSearch(indexName)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(QueryBuilders.fuzzyQuery("surfaceForm", searchString))
                .execute()
                .actionGet();
        log.info(response.toString());
        for (SearchHit sh : response.getHits()) {            
            result.put(sh.sourceAsMap().get("uri").toString(), sh.getScore());            
        }
        return result;
    }

    public static void testSearch() {
        try {
            IndexingModule im = new IndexingModule();
            System.out.println(im.searchIndex("bonapd"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testIndex() {
        try {
            IndexingModule im = new IndexingModule();
            im.insertToIndexByFile("/Users/ngonga/Downloads/qald_searchengine/dictionaryList/enTest.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
