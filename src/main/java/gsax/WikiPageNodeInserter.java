import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;
import java.lang.Exception;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.helpers.collection.MapUtil;

class WikiPageNodeInserter {
  private final static String NODE_FILE = "data/titles-sorted.txt";

  private enum Rels implements RelationshipType {
    LINKS_TO
  }

  public static void main(String[] args) {
    Map<String, String> config = new HashMap<String, String>();
    try {
      BufferedReader fin = new BufferedReader(new FileReader("config.txt"));
      String line;
      while ((line = fin.readLine()) != null) {
        int space = line.indexOf(" ");
        config.put(line.substring(0, space), line.substring(space + 1));
      }
    }
    catch (Exception e) {
      System.out.println("Can't read config file...");
      return;
    }

    // init neo4j db
    BatchInserter inserter = BatchInserters.inserter("db", config);
    BatchInserterIndexProvider index_provider =
      new LuceneBatchInserterIndexProvider(inserter);
    BatchInserterIndex pages =
      index_provider.nodeIndex("pages", MapUtil.stringMap("type", "exact"));
    System.out.println("Created database...");

    long i = 0;
    try {
      // create nodes and add to index
      BufferedReader fin = new BufferedReader(new FileReader(NODE_FILE));
      String line;
      Map<String, Object> properties = new HashMap<String, Object>();
      while ((line = fin.readLine()) != null) {
        properties.put("name", line);
        inserter.createNode(++i, properties);
        pages.add(i, properties);

        if (i % 100000 == 0) System.out.println("Created " + i + " nodes...");
      }

      fin.close();
      System.out.println("All nodes created and added to index...");
    }
    catch (Exception e) {
      System.out.println("Exception, failed at node " + i + "...");
    }

    pages.flush();
    index_provider.shutdown();
    inserter.shutdown();
  }
}
