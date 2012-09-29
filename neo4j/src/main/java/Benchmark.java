import java.io.File;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

public class Benchmark {


	GraphDatabaseService db;
	long start;

	void startDatabase() {
		db = new EmbeddedGraphDatabase("/tmp/neo4jbenchmark");
	}

	public static boolean deleteDir(File dir) {
	    if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i=0; i<children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }
	    return dir.delete();
	}
	
	void stopDatabase() {
		db.shutdown();
		deleteDir(new File("/tmp/neo4jbenchmark"));
	}

	void startChrono() {
		start = System.currentTimeMillis();
	}

	long getTime() {
		return System.currentTimeMillis() - start;
	}

	void create_and_retrieve_vertex() {
		Node node;
		Transaction tx = db.beginTx();
		try {
		    node = db.createNode();
		    tx.success();
		} finally {
		    tx.finish();
		}
		long id = node.getId();
		Node copy = db.getNodeById(id);
		assert copy.equals(node);
	}

	void create_1000_vertex() {
		Transaction tx = db.beginTx();
		try {
			for (int i = 0; i < 1000; i++) {
				db.createNode();
			}
			tx.success();
		} finally {
			tx.finish();
		}
	}

	void create_and_count_1000_vertex() {
		Transaction tx = db.beginTx();
		try {
			for (int i = 0; i < 1000; i++) {
				db.createNode();
			}
			tx.success();
		} finally {
			tx.finish();
		}
		GlobalGraphOperations operations = GlobalGraphOperations.at(db);
		Iterator<Node> iter = operations.getAllNodes().iterator();
		while (iter.hasNext()) {
			Node node = iter.next();
			node.getId();
		}
	}

	void create_1000_vertex_and_delete() {
		Transaction tx = db.beginTx();
		try {
			for (int i = 0; i < 1000; i++) {
				db.createNode();
			}
			tx.success();
		} finally {
			tx.finish();
		}
		tx = db.beginTx();
		try {
			GlobalGraphOperations operations = GlobalGraphOperations.at(db);
			Iterator<Node> iter = operations.getAllNodes().iterator();
			while (iter.hasNext()) {
				Node node = iter.next();
				node.delete();
			}
			tx.success();
		} finally {
			tx.finish();
		}
	}

	void create_relationship() {
		Transaction tx = db.beginTx();
		try {
			for (int i = 0; i < 1000; i++) {
				Node amirouche = db.createNode();
				Node neo4j = db.createNode();
				amirouche.createRelationshipTo(
						neo4j,
						DynamicRelationshipType.withName("knows")
				);
			}
			tx.success();
		} finally {
			tx.finish();
		}
	}

	void create_and_retrieve_relationship() {
		Transaction tx = db.beginTx();
		try {
			Node amirouche = db.createNode();
			Node neo4j = db.createNode();
			amirouche.createRelationshipTo(neo4j,
					DynamicRelationshipType.withName("knows"));
			tx.success();
		} finally {
			tx.finish();
		}
		tx = db.beginTx();
		try {
			GlobalGraphOperations operations = GlobalGraphOperations.at(db);
			Iterator<Relationship> iter = operations.getAllRelationships()
					.iterator();
			while (iter.hasNext()) {
				Relationship relationship = iter.next();
				relationship.delete();
			}
			tx.success();
		} finally {
			tx.finish();
		}
	}

	void create_complete_graph() {
		Transaction tx = db.beginTx();
		try {
			LinkedList<Node> nodes = new LinkedList<Node>();
			for (int i = 0; i < 1000; i++) {
				Node node = db.createNode();
				nodes.add(node);
			}
			Iterator<List<Node>> iter = new ListPermutation<Node>(nodes);
			while(iter.hasNext()) {
				List<Node> pair = iter.next();
				pair.get(0).createRelationshipTo(
						pair.get(1), 
						DynamicRelationshipType.withName("link")
				);
			}
			tx.success();
		} finally {
			tx.finish();
		}
	}

	public Node create_1000_relationships_and_count() {
		Transaction tx = db.beginTx();
		Node node;
		try {
			node = db.createNode();
			for (int i = 0; i<1000; i++) {
				Node other = db.createNode();
				node.createRelationshipTo(
						other, 
						DynamicRelationshipType.withName("link")
				);				
			}
			tx.success();
		} finally {
			tx.finish();
		}
		Iterator<Relationship> iter = node.getRelationships().iterator();
		Relationship relationship;
		while (iter.hasNext()) {
			relationship = iter.next();
		}
		return node;
	}
	
	public static void main(String [] args) {
		Benchmark benchmark = new Benchmark();
		long time;
		int i;

		Benchmark.deleteDir(new File("/tmp/neo4jbenchmarks"));
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_and_retrieve_vertex();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_and_retrieve_vertex * 1000 ");
		System.out.println(time);
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_1000_vertex();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_1000_vertex * 1000 ");
		System.out.println(time);
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_1000_vertex_and_delete();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_1000_vertex_and_delete * 1000 ");
		System.out.println(time);
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_and_count_1000_vertex();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_and_count_1000_vertex * 1000 ");
		System.out.println(time);
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_and_retrieve_relationship();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_and_retrieve_relationship * 1000 ");
		System.out.println(time);
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_relationship();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_relationship * 1000 ");
		System.out.println(time);
		
		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		benchmark.create_complete_graph();
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_complete_graph ");
		System.out.println(time);

		time = 0;
		benchmark.startDatabase();
		benchmark.startChrono();
		for(i=0; i<1000; i++) {
			benchmark.create_1000_relationships_and_count();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_1000_relationships_and_count ");
		System.out.println(time);
	
		time = 0;
		benchmark.startDatabase();
		Node node = benchmark.create_1000_relationships_and_count();
		benchmark.startChrono();
		Iterator<Relationship> iter = node.getRelationships().iterator();
		Relationship relationship;
		while (iter.hasNext()) {
			relationship = iter.next();
		}
		time = benchmark.getTime();
		benchmark.stopDatabase();
		System.out.print("create_1000_relationships_and_count ");
		System.out.println(time);		
	}
}
