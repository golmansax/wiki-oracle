import java.util.Map;
import java.util.HashMap;
import java.lang.Runtime;
import java.lang.Thread;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

class Test {
  private static GraphDatabaseService db_;
  private static Index<Node> pages_;

  private static enum Rels implements RelationshipType {
    LINKS_TO
  }

  final static TraversalDescription MY_TRAVERSAL =
    Traversal.description()
      .breadthFirst()
      .relationships(Rels.LINKS_TO, Direction.OUTGOING)
      .evaluator(Evaluators.toDepth(2))
      .uniqueness(Uniqueness.NODE_GLOBAL);

  public static void main(String[] args) {
    Map<String, String> config = new HashMap<String, String>();
    config.put("read_only", "true");
    db_ = new GraphDatabaseFactory()
      .newEmbeddedDatabaseBuilder("db")
      .setConfig(config)
      .newGraphDatabase();

    pages_ = db_.index().forNodes("pages");
    _RegisterShutdownHook(db_);
    System.out.println("Database loaded...");

    Transaction tx = db_.beginTx();
    String end = "Holman_Gao";
    boolean success = false;
    int i = 0;
    try {
      Node node = pages_.get("name", "Red_Hot_Chili_Peppers").getSingle();
      System.out.println("We have found " + node.getProperty("name"));

      for (Path p: MY_TRAVERSAL.traverse(node)) {
        if (p.endNode().getProperty("name").equals(end)) {
          System.out.println("Found path! ");
          for (Node n: p.nodes()) {
            System.out.println("\t" + n.getProperty("name"));
          }
          success = true;
          break;
        }
        if (++i % 1000 == 0) {
          System.out.println("Searched " + i + " paths, length: " + p.length());
        }
      }

      tx.success();
    }
    catch (Exception e) {
      System.out.println("Exception, fail...");
    }
    finally {
      tx.finish();
    }
    if (!success) {
      System.out.println("No path...");
    }

    db_.shutdown();
  }

  public static void _RegisterShutdownHook(final GraphDatabaseService db) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          db.shutdown();
        }
    });
  }
}
