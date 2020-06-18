package FromHbaseToNeo4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.neo4j.cypher.internal.compiler.v2_3.No;
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str;
import org.neo4j.graphdb.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FromHbase {
    //日志记录
    private static Logger logger = Logger.getLogger(FromHbase.class);
    
    //与HBase数据库的连接对象
    public static Connection connection;

    //数据库元数操作对象
    public static Admin admin;
    
    /**
     * @ClassName : FromHbase
     * @Description : hbase 链接设置
     * @param

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/1
    */
    public void setup() throws IOException {
        logger.info("开始Hbase链接配置");
        //System.out.println("开始Hbase链接配置");
        //取得一个数据库配置参数对象
        Configuration conf =HBaseConfiguration.create();
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        logger.info("Hbase链接配置完成");
    }


    /**
     * @ClassName : FromHbase
     * @Description : 通过表名查询表中的所有数据
     * @param tableNameString  表名，通过args[]获取

     * @Return : void
     * @Author : ZNWhahaha
     * @Date : 2020/6/1
    */
    public void queryTable(String tableNameString) throws IOException {
        logger.info("********开始查询整表数据********");
        //System.out.println("********开始查询整表数据*******");
        String name = "";
        String nameEN = "";
        String orgName = "";
        String orgMessage = "";
        String personID="";
        String personID2 = "";
        String personType = "";
        List<String> paperID = new ArrayList<String>();
        List<String> paperTitle = new ArrayList<String>();

        //进行neo4j数据库的创建
        ToNeo4j neo = new ToNeo4j();
        //创建链接neo4j数据库
        neo.init("/Users/znw_mac/App/neo4j-community-3.5.18/data/databases/graph.db");

        //获取数据表对象
        Table table = connection.getTable(TableName.valueOf(tableNameString));

        //获取表中的数据
        ResultScanner scanner = table.getScanner(new Scan());



        //循环输出表中的数据
        for (Result result : scanner) {

            String row = Bytes.toString(result.getRow());
            if (row.contains("->")) {
                name = row.split("->")[0];
                orgName = row.split("->")[1];
            }
            else {
                name = row;
                orgName = "";
            }

            logger.info("row key is:"+row);
            logger.info("name : "+name);
            logger.info("orgName : "+ orgName);

            paperID.clear();
            paperTitle.clear();
            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells) {
                //取值，测试用
                //logger.info("cloneQualifier name is " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                //logger.info("row value is:" + Bytes.toString(CellUtil.cloneValue(cell)));


                //取对应值
                if ("nameEN".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                     nameEN = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("orgMessage".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                     orgMessage = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("personID".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    personID = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("personID2".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    personID2 = Bytes.toString(CellUtil.cloneValue(cell));
                }
                else if ("paper".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                    paperID.add(Bytes.toString(CellUtil.cloneFamily(cell)));
                    paperTitle.add(Bytes.toString(CellUtil.cloneValue(cell)));

                }
                else {
                    personType = Bytes.toString(CellUtil.cloneValue(cell));
                }
            }
            logger.info("Message are :" +nameEN +"  "+orgMessage+"  "+ personID+" "+personID2+"  "+paperID+"  "+paperTitle+"  "+personType);


            //创建Person节点
            Node personnode = neo.createPersonNode(name,nameEN,personID,personID2);
            //创建Paper节点
            List<Node> papernode = new ArrayList<Node>();
            for (int i = 0; i < paperID.size(); i++) {
                papernode.add(neo.createPaperNode(paperTitle.get(i),paperID.get(i)));
            }

            //创建Org节点
            Node orgndoe = neo.searchOrg(orgName);
            if (orgName.equals("") || orgndoe == null) {
                Node orgnode1 = neo.createOrgNode(orgName, orgMessage);
                orgndoe = orgnode1;
            }

            //添加Person与Org依赖关系
            neo.createRel(orgndoe,personnode,0);
            //添加Paper与Person依赖
            for (Node ne : papernode)
            neo.createRel(ne,personnode,1);
            

        }
        //System.out.println("********查询整表数据结束********");
        logger.info("********转存数据结束********");
    }


}
