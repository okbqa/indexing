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
import java.util.Map;
import java.util.Set;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;

public class IndexingModule {

    String clientIp = "143.248.135.212";
    int port = 9300;
    String indexName = "okbqa";
    Set<String> whiteList = null;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        // on startup
        try {

            IndexingModule im = new IndexingModule();
            //im.insertToIndexByFile("C:/workspace/qald_searchengine/dictionaryList/enTest.txt");

            if (args.length > 1) {
                im.indexName = args[1];
            }
            if (args.length > 2) {
                im.clientIp = args[2];
                im.port = Integer.parseInt(args[3]);
                im.indexName = args[4];
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

    /** Reads the white list from a file (one URI per line)
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

    /** Reads a TSV file of surface forms and write it in the index
     * 
     * @param fileName TSV File
     * @throws ElasticsearchException If server does not work
     * @throws IOException If file cannot be read
     */
    public void insertToIndexByFile(String fileName) throws ElasticsearchException, IOException {
        Client client = getClient();
        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
        String uri, surfaceForm, str;
        int index = 0;
        boolean indexing;
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
                    IndexResponse response = client.prepareIndex(indexName, "label", "" + index)
                            .setSource(json)
                            .execute()
                            .actionGet();
                    index++;
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
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", indexName).build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient = transportClient.addTransportAddress(new InetSocketTransportAddress(clientIp, port));
        return (Client) transportClient;
    }
    
    
}
