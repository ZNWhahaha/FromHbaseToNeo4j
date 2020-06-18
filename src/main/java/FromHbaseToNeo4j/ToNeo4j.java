package FromHbaseToNeo4j;

import org.apache.log4j.Logger;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;

public class ToNeo4j {
    //日志
    private static Logger logger = Logger.getLogger(ToNeo4j.class);

    //数据库实例
    private GraphDatabaseService graphDB;

    /**
     * @ClassName : ToNeo4j
     * @Description :创建关系类型
     * @param 

     * @Return :
     * @Author : ZNWhahaha
     * @Date : 2020/6/10
    */
    private enum RelTypes implements RelationshipType {
        Contain,belongto
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建节点类型
     * @param

     * @Return :
     * @Author : ZNWhahaha
     * @Date : 2020/6/10
    */
    public enum NodeType implements Label {
        Author,Organization,Paper
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 初始化Neo4j数据库
     * @param

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
    */
    public void init(String path) {
        graphDB = new GraphDatabaseFactory().newEmbeddedDatabase(new File(path));
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建Person节点
     * @param nameEn
     * @param name
     * @param personID
     * @param personID2
     * @Return : Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
    */
    public Node createPersonNode(String name,String nameEn,String personID,String personID2){
        logger.info("开始创建PersonNode : "+ name);
        Transaction tx = graphDB.beginTx();
        
        Node node = graphDB.createNode(NodeType.Author);
        node.setProperty("name", name);
        node.setProperty("nameEn", nameEn);
        if (personID.equals(personID2)){
            node.setProperty("personID", personID);
        }
        else {
            node.setProperty("personID", personID);
            node.setProperty("personID2", personID2);
        }
        logger.info("PersonNode创建结束");
        tx.success();
        tx.close();
        return node;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建Paper节点
     * @param papername
     * @param paperID
     * @Return : Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
    */
    public Node createPaperNode(String papername,String paperID){
        logger.info("开始创建PaperNode : " + papername);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.createNode(NodeType.Paper);
        node.setProperty("papername", papername);
        node.setProperty("paperID", paperID);
        tx.success();
        tx.close();
        logger.info("创建PaperNode结束");
        return node;
    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 创建org节点
     * @param orgname
     * @param orgmessage
     * @Return : Node
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
    */
    public Node createOrgNode(String orgname,String orgmessage){
        logger.info("开始创建OrgNode : " + orgname);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.createNode(NodeType.Organization);
        node.setProperty("orgname", orgname);
        if(orgmessage.contains(",")){
            node.setProperty("orgmessage", orgmessage.split(",")[0]);
            node.setProperty("orgpostcode", orgmessage.split(",")[1]);
        }
        else {
            node.setProperty("orgmessage", orgmessage);
        }
        logger.info("创建OrgNode结束");
        tx.success();
        tx.close();
        return node;

    }

    /**
     * @ClassName : ToNeo4j
     * @Description : 对于相应节点创建对应关系
     * @param node_1
     * @param node_2
     * @param num
     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
    */
    public void createRel(Node node_1,Node node_2,int num){
        logger.info("开始创建关系 "+node_1.getId());
        Transaction tx = graphDB.beginTx();
        node_1.createRelationshipTo(node_2,RelTypes.values()[num]);
        tx.success();
        tx.close();
        logger.info("创建关系结束");
    }

    
    /**
     * @ClassName : ToNeo4j
     * @Description : 通过节点名进行查询，验证是否存在
     * @param : name

     * @Return : java.lang.Boolean
     * @Author : ZNWhahaha
     * @Date : 2020/6/18
    */
    public Node searchOrg(String name){
        logger.info("开始查找OrgNode : " +name);
        Transaction tx = graphDB.beginTx();
        Node node = graphDB.findNode(NodeType.Organization, "name", name);
        tx.success();
        tx.close();
        logger.info("查找OrgNode结束");
        return node;
    }

}
