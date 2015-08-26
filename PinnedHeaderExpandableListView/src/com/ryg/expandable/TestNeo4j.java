package com.ryg.expandable;

import java.util.List;


public class TestNeo4j {
  public static void main(String[] args) {
    Neo4jService nj = new Neo4jService();
    Thread t = new Thread(nj,"Neo4jThread");
    t.start();
    nj.addNode(0);
    nj.addNode(1);
    nj.addNode(2);
    nj.addNode(3);
    nj.addNode(4);
    nj.addNode(5);
    nj.addNode(6);
    nj.addNode(7);
    nj.addNode(8);
    nj.addNode(9);
    nj.addNode(10);
    
    
    nj.addNodeRelationShip(0,1);
    nj.addNodeRelationShip(0,2);
    nj.addNodeRelationShip(0,3);
    nj.addNodeRelationShip(1,4);
    nj.addNodeRelationShip(1,5);
    nj.addNodeRelationShip(1,6);
    
    nj.addNodeRelationShip(2,7);
    nj.addNodeRelationShip(2,9);
    
    nj.addNodeRelationShip(3,8);
    nj.addNodeRelationShip(3,10);
    
    nj.addNodeRelationShip(8,9);
    nj.addNodeRelationShip(10,9);
    
    List<Integer> sons = nj.getJobFirstParents(9);
    for(Integer i : sons) {
      System.out.println(i);
    }
    
    nj.deleteNodeRelationShip(2, 9);
    
    sons = nj.getJobFirstParents(9);
    System.out.println("after delete 2--9");
    for(Integer i : sons) {
      System.out.println(i);
    }
    
    nj.setRunning(false);
    
    
  }
}