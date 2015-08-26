package com.ryg.expandable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.impl.util.FileUtils;

/**
 * Neo4j服务类
 * @author lxw
 *
 */
public class Neo4jService implements Runnable 
{
  
  private static Logger log = Logger.getLogger(Neo4jService.class);
  
  private final String DB_PATH = "neo4jdb/skynet-neo4j.db";
  
  private GraphDatabaseService db;
  private Index<Node> index = null;
  
  public static final String INDEX_ID_KEY = "idx_id";
  public static final String JOB_ID_KEY = "job_id";
  
  private boolean running = true;
  
  

  private enum RelTypes implements RelationshipType{
  KNOWS,
    }
  
  public Neo4jService() {	
    init();
  }
  
  public void init() {
    clearDB();
    db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
    registerShutdownHook();
    Transaction tx = db.beginTx();
    try {
      this.index = db.index().forNodes("jobNodes");
      tx.success();
    } catch (Exception e) {
      log.error(e);
      log.error("init() error.."); 
    } finally {
      tx.close();
    }
  }
  
  //添加节点
  public boolean addNode(int job_id) {
    Transaction tx = db.beginTx();
    try {
      Node jobNode = db.createNode();
      jobNode.setProperty(JOB_ID_KEY, job_id);
      this.index.add(jobNode, INDEX_ID_KEY, job_id);
      tx.success();
      log.info("addNode: [" + job_id + "].");
    } catch (Exception e) {
      log.error(e);
      log.error("addNode() error ..");
      return false;
    } finally {
      tx.close();
    }
    return true;
  }
  
  /**
   * 删除节点依赖
   * @param parent_job_id
   * @param son_job_id
   * @return
   */
  public boolean deleteNodeRelationShip(int parent_job_id,int son_job_id) {
    Transaction tx = db.beginTx();
    try {
      Node parentJobNode = getNodeByJob(parent_job_id);
      Node sonJobNode = getNodeByJob(son_job_id);
      Iterable<Relationship> relationships = parentJobNode.getRelationships(RelTypes.KNOWS, Direction.OUTGOING);
      for(Relationship r : relationships) {
        if(r.getEndNode().equals(sonJobNode)) {
          r.delete();
        }
      }
      tx.success();
      log.info("deleteNodeRelationShip: [" + parent_job_id + "," + son_job_id + "].");
    } catch (Exception e) {
      log.error(e);
      log.error("deleteNodeRelationShip() error .. paren_job_id is [" + parent_job_id 
          + "], son_job_id is [" + son_job_id + "]."); 
    } finally {
      tx.close();
    }
    return false;
  }
  
  //删除节点
  public boolean deleteNode(int job_id) {		
    Transaction tx = db.beginTx();
    try {
      Node jobNode = getNodeByJob(job_id);
      jobNode.delete();
      tx.success();
      log.info("deleteNode: [" + job_id + "].");
    } catch (Exception e) {
      log.error(e);
      log.error("deleteNode() error ..");
      return false;
    } finally {
      tx.close();
    }
    return true;
  }
  
  //添加节点依赖
  public boolean addNodeRelationShip(int parent_job_id,int son_job_id) {
    Transaction tx = db.beginTx();
    try {
      Node parentJobNode = getNodeByJob(parent_job_id);
      Node sonJobNode = getNodeByJob(son_job_id);
      parentJobNode.createRelationshipTo(sonJobNode, RelTypes.KNOWS);
      tx.success();
      log.info("addNodeRelationShip: [" + parent_job_id + "," + son_job_id + "].");
    } catch (Exception e) {
      log.error(e);
      log.error("addNodeRelationShip() error .. paren_job_id is [" + parent_job_id 
          + "], son_job_id is [" + son_job_id + "]."); 
    } finally {
      tx.close();
    }
    return false;
  }
  
  //获取节点的子任务，包括自身
  public List<Integer> getJobSons (int job_id,int depth) {
    List<Integer> jobSons = new ArrayList<Integer>();
    PathEvaluator<?> path = null;
    if(depth == 0 || depth == -1) {
      path = Evaluators.excludeStartPosition();
    } else {
      path = Evaluators.toDepth(depth);
    }
    Node jobNode = getNodeByJob(job_id);
    
    if(null != jobNode) {
      Transaction tx = db.beginTx();
      try {
        TraversalDescription td = db.traversalDescription()
        .breadthFirst()
        .relationships( RelTypes.KNOWS, Direction.OUTGOING )
        .evaluator(path);
        Traverser t = td.traverse(jobNode);
        
        for(Path p : t) {
          log.info("level:" + p.length());
          log.info("id:" + p.endNode().getProperty(JOB_ID_KEY));
          jobSons.add((Integer) p.endNode().getProperty(JOB_ID_KEY));
        }
        tx.success();
      } catch (Exception e) {
        log.error(e);
        log.error("getJobSons error .. job_id is [" + job_id + "]");
      } finally {
        tx.close();
      }
    }
    return jobSons;
  }
  
  
  //获取节点的父任务，包括自身
  public List<Integer> getJobParents (int job_id,int depth) {
    List<Integer> jobParents = new ArrayList<Integer>();
    PathEvaluator<?> path = null;
    if(depth == 0 || depth == -1) {
      path = Evaluators.excludeStartPosition();
    } else {
      path = Evaluators.toDepth(depth);
    }
    Node jobNode = getNodeByJob(job_id);
    
    if(null != jobNode) {
      Transaction tx = db.beginTx();
      try {
        TraversalDescription td = db.traversalDescription()
        .breadthFirst()
        .relationships( RelTypes.KNOWS, Direction.INCOMING )
        .evaluator(path);
        Traverser t = td.traverse(jobNode);
        
        for(Path p : t) {
          log.info("level:" + p.length());
          log.info("id:" + p.endNode().getProperty(JOB_ID_KEY));
          jobParents.add((Integer) p.endNode().getProperty(JOB_ID_KEY));
        }
        tx.success();
      } catch (Exception e) {
        log.error(e);
        log.error("getJobParents error .. job_id is [" + job_id + "]");
      } finally {
        tx.close();
      }
    }
    return jobParents;
  }
  
  
  //获取节点的一级父任务,不包括自身
  public List<Integer> getJobFirstParents (int job_id) {
    List<Integer> jobParents = new ArrayList<Integer>();
    PathEvaluator<?> path = Evaluators.atDepth(1);
    Node jobNode = getNodeByJob(job_id);
    
    if(null != jobNode) {
      Transaction tx = db.beginTx();
      try {
        TraversalDescription td = db.traversalDescription()
        .breadthFirst()
        .relationships( RelTypes.KNOWS, Direction.INCOMING )
        .evaluator(path);
        Traverser t = td.traverse(jobNode);
        
        for(Path p : t) {
          log.info("level:" + p.length());
          log.info("id:" + p.endNode().getProperty(JOB_ID_KEY));
          jobParents.add((Integer) p.endNode().getProperty(JOB_ID_KEY));
        }
        tx.success();
      } catch (Exception e) {
        log.error(e);
        log.error("getJobFirstParents error .. job_id is [" + job_id + "]");
      } finally {
        tx.close();
      }
    }
    return jobParents;
  }
  
  //获取节点的一级子任务，不包括自身
  public List<Integer> getJobFirstSons (int job_id) {
    List<Integer> jobSons = new ArrayList<Integer>();
    PathEvaluator<?> path = Evaluators.atDepth(1);
    Node jobNode = getNodeByJob(job_id);
    
    if(null != jobNode) {
      Transaction tx = db.beginTx();
      try {
        TraversalDescription td = db.traversalDescription()
        .breadthFirst()
        .relationships( RelTypes.KNOWS, Direction.OUTGOING )
        .evaluator(path);
        Traverser t = td.traverse(jobNode);
        
        for(Path p : t) {
          log.info("level:" + p.length());
          log.info("id:" + p.endNode().getProperty(JOB_ID_KEY));
          jobSons.add((Integer) p.endNode().getProperty(JOB_ID_KEY));
        }
        tx.success();
      } catch (Exception e) {
        log.error(e);
        log.error("getJobFirstParents error .. job_id is [" + job_id + "]");
      } finally {
        tx.close();
      }
    }
    return jobSons;
  }
  
  //根据job_id获取Node
  public Node getNodeByJob(int job_id) {
    Node node = null;
    Transaction tx = db.beginTx();
    try {
      node = this.index.get(INDEX_ID_KEY,job_id).getSingle();
      tx.success();
    } catch (Exception e) {
      log.error(e);
      log.error("getNodeByJob() error, job_id is [" + job_id + "].");
    } finally {
      tx.close();
    }
    return node;
  }
  
  private void registerShutdownHook() {
  // Registers a shutdown hook for the Neo4j instance so that it
  // shuts down nicely when the VM exits (even if you "Ctrl-C" the
  // running example before it's completed)
  Runtime.getRuntime()
    .addShutdownHook( new Thread()
    {
        @Override
        public void run()
        {
        	log.info("neo4j shutdown hook ... ");
      db.shutdown();
        }
    } );
    }
  
  private void clearDB() {  
  try {  
      FileUtils.deleteRecursively(new File(DB_PATH));  
  }  
  catch(IOException e) {  
      throw new RuntimeException(e);  
  }  
    }

  @Override
  public void run() {
    while (running) {
      log.info(new Date() + " ### Neo4jService 运行正常！");
      try {
        Thread.sleep(20000);
      } catch (InterruptedException e) {
        log.error(e);
      }
    }
  }  
  
  public void setRunning(boolean running) {
    this.running = running;
  }
  
  
}