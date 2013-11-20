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

class TestInserter {
  private final static String NODE_FILE = "data/titles-sorted.txt";

  private enum Rels implements RelationshipType {
    KNOWS
  }

  public static void main(String[] args) {
    // init neo4j db
    BatchInserter inserter = BatchInserters.inserter("test_db");

    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put("name", "holman");
    inserter.createNode(1, properties);
    properties.put("name", "jefferson");
    inserter.createNode(2, properties);
    inserter.createRelationship(1, 2, Rels.KNOWS, null);

    inserter.shutdown();
  }
}
