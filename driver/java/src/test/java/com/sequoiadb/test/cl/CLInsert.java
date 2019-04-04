package com.sequoiadb.test.cl;

import com.sequoiadb.base.*;
import com.sequoiadb.exception.BaseException;
import com.sequoiadb.exception.SDBError;
import com.sequoiadb.test.common.Constants;
import com.sequoiadb.testdata.SDBTestHelper;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.junit.*;

import static com.sequoiadb.base.DBCollection.FLG_INSERT_CONTONDUP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class CLInsert {
    private static Sequoiadb sdb;
    private static CollectionSpace cs;
    private static DBCollection cl;
    private static DBCursor cursor;
    private static long i = 0;

    @BeforeClass
    public static void setConnBeforeClass() throws Exception {
        // sdb
        sdb = new Sequoiadb(Constants.COOR_NODE_CONN, "", "");

        // cs
        if (sdb.isCollectionSpaceExist(Constants.TEST_CS_NAME_1)) {
            sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
            cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
        } else
            cs = sdb.createCollectionSpace(Constants.TEST_CS_NAME_1);
        // cl
        BSONObject conf = new BasicBSONObject();
        conf.put("ReplSize", 0);
        cl = cs.createCollection(Constants.TEST_CL_NAME_1, conf);
    }

    @AfterClass
    public static void DropConnAfterClass() throws Exception {
        sdb.dropCollectionSpace(Constants.TEST_CS_NAME_1);
        sdb.disconnect();
    }

    @Before
    public void setUp() throws Exception {
        cl.truncate();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void insertOneRecord() {
        System.out.println("begin to test insertOneRecord ...");

        // create record
        BSONObject obj = new BasicBSONObject();
        BSONObject obj1 = new BasicBSONObject();
        ObjectId id = new ObjectId();
        ;
        obj.put("_id", id);
        obj.put("Id", 10);
        obj.put("姓名", "汤姆");
        obj.put("年龄", 30);
        obj.put("Age", 30);

        obj1.put("0", "123456");
        obj1.put("1", "654321");

        obj.put("电话", obj1);
        obj.put("boolean1", true);
        obj.put("boolean2", false);
        obj.put("nullobj", null);
        obj.put("intnum", 999999999);
//		obj.put("floatnum",9999999999.9999999999);

        // record current session totalInsert num
        BSONObject temp = new BasicBSONObject();
        DBCursor cur = sdb.getSnapshot(3, temp, temp, temp);
        long time1 = SDBTestHelper.getTotalBySnapShotKey(cur, "TotalInsert");
        System.out.println("before insert, current session total insert num is " + time1);
        long count = 0;
        count = cl.getCount();
        System.out.println("before insert, the count is: " + count);
        // insert
        cl.insert(obj);
        long count1 = 0;
        count1 = cl.getCount();
        System.out.println("after insert, the count is: " + count1);
        BSONObject empty = new BasicBSONObject();
        cur = sdb.getSnapshot(3, empty, empty, empty);
        long time2 = SDBTestHelper.getTotalBySnapShotKey(cur, "TotalInsert");
        System.out.println("after insert, current session total insert num is " + time2);
        assertEquals(1, time2 - time1);

        //query without condition
        BSONObject tmp = new BasicBSONObject();
        DBCursor tmpCursor = cl.query(tmp, null, null, null);
        while (tmpCursor.hasNext()) {
            BSONObject temp1 = tmpCursor.getNext();
            System.out.println(temp1.toString());
        }

        // query
        BSONObject query = new BasicBSONObject();
        query.put("Id", 10);
        query.put("姓名", "汤姆");
        query.put("年龄", 30);
        query.put("Age", 30);
        query.put("电话.0", "123456");
        query.put("电话.1", "654321");
        query.put("boolean1", true);
        query.put("boolean2", false);
        query.put("nullobj", null);
        query.put("intnum", 999999999);
//		query.put("floatnum",9999999999.9999999999);
        cursor = cl.query(query, null, null, null);
        while (cursor.hasNext()) {
            BSONObject temp1 = cursor.getNext();
            System.out.println(temp1.toString());
            i++;
        }
        long count2 = cl.getCount();
        System.out.println("after cl.query(), i is: " + i);
        System.out.println("after cl.query(), the count is: " + count2);
        // check
        assertEquals(1, i);
    }

    @Test
    public void insertNumberLong() {
        System.out.println("begin to test insertNumberLong ...");

        String json = "{a:{$numberLong:\"10000000\"}}";
        String result = "10000000";

        cl.insert(json);
        BSONObject qobj = new BasicBSONObject();
        DBCursor cursor = cl.query(qobj, null, null, null);
        while (cursor.hasNext()) {
            BSONObject record = cursor.getNext();
            // check
            assertTrue(record.toString().indexOf(result) >= 0);
        }
    }

    @Test
    public void insertWithFlag() {
        BSONObject obj1 = new BasicBSONObject().append("_id", 1).append("a",1);
        BSONObject obj2 = new BasicBSONObject().append("_id", 1).append("a",1);
        cl.insert(obj1, FLG_INSERT_CONTONDUP);
        cl.insert(obj2, FLG_INSERT_CONTONDUP);
        try {
            cl.insert(obj2, 0);
            Assert.fail();
        } catch (BaseException e) {
            Assert.assertEquals(SDBError.SDB_IXM_DUP_KEY.getErrorCode(), e.getErrorCode());
        }
    }
}
