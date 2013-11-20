import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.lang.Exception;
import java.lang.Long;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.helpers.collection.MapUtil;

class WikiPageRelInserter {
  private final static String REL_FILE = "data/links-simple-sorted.txt";

  private static enum Rels implements RelationshipType {
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

    // insert all relationships
    // let's add them 1M at a time (b/c of possible memory constraints)
    long i = 0;
    int n_dumps = 0;
    BatchInserter inserter = null;

    try {
      BufferedReader fin = new BufferedReader(new FileReader(REL_FILE));
      String line;
      while ((line = fin.readLine()) != null) {
        if (inserter == null) {
          inserter = BatchInserters.inserter("db", config);
          System.out.println("Loaded database...");
        }

        int colon = line.indexOf(":");
        long id = Long.parseLong(line.substring(0, colon));
        StringTokenizer st = new StringTokenizer(line.substring(colon + 2));
        while (st.hasMoreTokens()) {
          long nei = Long.parseLong(st.nextToken());
          inserter.createRelationship(id, nei, Rels.LINKS_TO, null);
          if (++i % 100000 == 0) System.out.println("Created " + i + " rels...");
        }

        // dump to database
        if (i > 50000000 && inserter != null) {
          inserter.shutdown();
          inserter = null;
          System.out.println("Made " + (++n_dumps) + " dumps...");
          i = 0;
        }
      }

      fin.close();
      System.out.println("All relationships added...");
    }
    catch (Exception e) {
      System.out.println("Exception, failed at node " + i + "...");
    }

    if (inserter != null) inserter.shutdown();
  }
}
